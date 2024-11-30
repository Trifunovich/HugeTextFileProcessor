using System.Buffers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace TextFile.Parser;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class ParallelBackgroundWorkers(IConfiguration configuration, ILogger<ParallelBackgroundWorkers> logger) : ParserBase(configuration)
{
    private static int mmIndex = 0;

    public override async Task CreateExternalChunks()
    {

        if (!File.Exists(InputFile))
        {
            throw new FileNotFoundException($"The file {InputFile} does not exist.");
        }

        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<(int, string)>(boundedCapacity: BoundedCap);
        var filesQueue = new BlockingCollection<string>(new ConcurrentStack<string>());
        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTask = Task.Run(() => ProcessLinesAsync(0, linesQueue, ChunkFolder, filesQueue));
        var microMergingTask = Task.Run(() => MicroMergeAsync(filesQueue, 0));

        await readingTask;
        await processingTask;
        filesQueue.CompleteAdding(); // Close the filesQueue after processingTask is done

        await microMergingTask;
    }

    public override Task MergeSortedChunks()
    {
        return Task.CompletedTask;
    }

    private async Task MergeFiles(string[] sortedFiles, string outputFile)
    {
        try
        {
            var readers = sortedFiles.Select(file => new StreamReader(file)).ToList();
            var priorityQueue = new PriorityQueue<QueueItem, string>();

            foreach (var reader in readers.Where(reader => reader.Peek() >= 0))
            {
                var line = await reader.ReadLineAsync();
                if (line != null)
                {
                    priorityQueue.Enqueue(new QueueItem { Line = line, Reader = reader }, line);
                }
            }

            await using (var fileStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write))
            await using (var bufferedWriter = new BufferedStream(fileStream))
            await using (var writer = new StreamWriter(bufferedWriter))
            {
                while (priorityQueue.Count > 0)
                {
                    var value = priorityQueue.Dequeue();
                    await writer.WriteLineAsync(value.Line);

                    if (value.Reader.Peek() >= 0)
                    {
                        var line = await value.Reader.ReadLineAsync();
                        if (line != null)
                        {
                            priorityQueue.Enqueue(new QueueItem { Line = line, Reader = value.Reader }, line);
                        }
                    }
                    else
                    {
                        value.Reader.Dispose();
                    }
                }
            }

            foreach (var file in sortedFiles)
            {
                File.Delete(file);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error while merging files");
        }
    }


    private class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; set; }
    }


    private static async Task ReadLinesAsync(string inputFile, BlockingCollection<(int, string)> linesQueue)
    {
        using (var reader = new StreamReader(inputFile))
        {
            var buffer = ArrayPool<char>.Shared.Rent(BulkWriteSize);
            try
            {
                int bytesRead;
                while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    var lines = new string(buffer, 0, bytesRead).Split(new[] { "\r\n", "\n" }, StringSplitOptions.TrimEntries);
                    foreach (var line in lines)
                    {
                        var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.TrimEntries);
                        if (parts.Length == 2 && int.TryParse(parts[0], out var number))
                        {
                            linesQueue.Add((number, parts[1]));
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<char>.Shared.Return(buffer);
            }
        }

        linesQueue.CompleteAdding();
    }

    private async Task ProcessLinesAsync(int ind, BlockingCollection<(int, string)> linesQueue, string chunkFolder,
        BlockingCollection<string> filesQueue)
    {
        var records = new List<Record>();
        var chunkIndex = 0;
        foreach (var line in linesQueue.GetConsumingEnumerable())
        {
            ProcCount[ind]++;
            records.Add(new Record { Number = line.Item1, Text = line.Item2 });

            if (records.Count < ChunkSize) continue;
            await WriteChunkToFileAsync(records, chunkIndex++, ind, chunkFolder, filesQueue);
            records.Clear();
        }

        if (records.Count > 0)
        {
            await WriteChunkToFileAsync(records, chunkIndex, ind, chunkFolder, filesQueue);
        }

        logger.LogInformation("Task {Index} has processed {Qty} lines", ind, ProcCount[ind]);
    }

    private static readonly Lock FileLock = new();

    private async Task MicroMergeAsync(BlockingCollection<string> filesQueue, int ind)
    {
        while (true)
        {
            while (filesQueue.Count < 2)
            {
                if (!filesQueue.IsAddingCompleted)
                {
                    await Task.Delay(100);
                }
                else
                {
                    logger.LogInformation("Final merge will be done later");
                    logger.LogInformation("Task {Index} has processed {Qty} lines", ind, MmCount[ind]);
                    return;
                }
            }

            string file1, file2;
            lock (FileLock)
            {
                if (filesQueue.Count < 2)
                {
                    continue;
                }
                file1 = filesQueue.Take();
                file2 = filesQueue.Take();
            }

            var newFileName = GenerateNewFileName(file1, mmIndex++);

            if (file2 != null)
            {
                await MergeFiles(new[] { file1, file2 }, newFileName);
            }
            else
            {
                File.Move(file1, OutputFile);
                return;
            }

            if (!filesQueue.IsAddingCompleted)
            {
                filesQueue.Add(newFileName);
            }

            logger.LogDebug("File {FilePath} merged out of {Fl1} and {Fl2}", newFileName, file1, file2);
        }
    }


    private string GenerateNewFileName(string originalFileName, int index)
    {
        var lastChunkIndex = originalFileName.LastIndexOf("chunk", StringComparison.Ordinal);
        var baseName = originalFileName.Substring(0, lastChunkIndex + "chunk".Length);
        return $"{baseName}_mm{index}.txt";
    }

    private static async Task WriteChunkToFileAsync(List<Record> records, int chunkIndex, int ind, string chunkFolder,
        BlockingCollection<string> filesQueue)
    {
        records.Sort((x, y) =>
        {
            var textComparison = string.Compare(x.Text, y.Text, StringComparison.Ordinal);
            return textComparison != 0 ? textComparison : x.Number.CompareTo(y.Number);
        });

        var chunkFileName = Path.Combine(chunkFolder, $"chunk_{chunkIndex}_{ind}.txt");
        await using var writer = new StreamWriter(chunkFileName);
        foreach (var record in records)
        {
            await writer.WriteLineAsync(record.ToString());
        }
        filesQueue.Add(chunkFileName);
    }

    private class Record
    {
        public int Number { get; init; }
        public string Text { get; init; }

        public override string ToString()
        {
            return $"{Number}. {Text}";
        }
    }
}
