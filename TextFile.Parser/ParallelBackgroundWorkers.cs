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
        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<(int, string)>(boundedCapacity: BoundedCap);
        var filesQueue = new BlockingCollection<string>(new ConcurrentStack<string>());
        var processors = Environment.ProcessorCount;
        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTasks = Enumerable.Range(0, processors).Select(x => Task.Run(() =>
        {
            ProcCount[x] = 0;
            return ProcessLinesAsync(x, linesQueue, ChunkFolder, filesQueue);
        })).ToArray();
        var microMergingTasks = Enumerable.Range(0, processors).Select(x => Task.Run(() =>
        {
            MmCount[x] = 0;
            return MicroMergeAsync(filesQueue, x);
        })).ToArray();

        await Task.WhenAll(new[] { readingTask }.Concat(processingTasks));
        filesQueue.CompleteAdding(); // Close the filesQueue after processingTasks are done

        await Task.WhenAll(microMergingTasks);
    }

    public override async Task MergeSortedChunks()
    {
        var sortedFiles = Directory.GetFiles(ChunkFolder, "chunk_*.txt");
        await MergeFiles(sortedFiles, OutputFile);
    }

    private async Task MergeFiles(string[] sortedFiles, string outputFile)
    {
        try
        {
            var readers = sortedFiles.Select(file => new StreamReader(file)).ToList();
            var queue = new SortedDictionary<string, QueueItem>();

            foreach (var reader in readers.Where(reader => reader.Peek() >= 0))
            {
                var line = await reader.ReadLineAsync();
                if (line != null) queue.Add(line, new QueueItem { Line = line, Reader = reader });
            }

            await using (var writer = new StreamWriter(outputFile))
            {
                while (queue.Count > 0)
                {
                    var (s, value) = queue.First();
                    await writer.WriteLineAsync(s);

                    queue.Remove(s);

                    if (value.Reader.Peek() >= 0)
                    {
                        var line = await value.Reader.ReadLineAsync();
                        if (line != null) queue.Add(line, new QueueItem { Line = line, Reader = value.Reader });
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

    private static async Task ReadLinesAsync(string? inputFile, BlockingCollection<(int, string)> linesQueue)
    {
        if (!File.Exists(inputFile))
        {
            throw new FileNotFoundException($"The file {inputFile} does not exist.");
        }

        using (var reader = new StreamReader(inputFile))
        {
            var buffer = new char[BulkWriteSize];
            int bytesRead;
            while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                var lines = new string(buffer, 0, bytesRead).Split(["\r\n", "\n"], StringSplitOptions.TrimEntries);
                foreach (var line in lines)
                {
                    var parts = line.Split([". "], 2, StringSplitOptions.TrimEntries);
                    if (parts.Length == 2 && int.TryParse(parts[0], out var number))
                    {
                        linesQueue.Add((number, parts[1]));
                    }
                }
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
            records.Add(new Record {Number = line.Item1, Text = line.Item2});

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

    private async Task MicroMergeAsync(BlockingCollection<string> filesQueue, int ind)
    {
        while (true)
        {
            while (filesQueue.Count < 3)
            {
                if (!filesQueue.IsAddingCompleted)
                {
                    await Task.Delay(100);
                }
                else
                {
                    logger.LogInformation("There final merge will be done later");
                    logger.LogInformation("Task {Index} has processed {Qty} lines", ind, MmCount[ind]);
                    return;
                }
            }

            var file1 = filesQueue.Take();
            var file2 = filesQueue.Take();

            var newFileName = GenerateNewFileName(file1, mmIndex++);

            await MergeFiles([file1, file2], newFileName);
            filesQueue.Add(newFileName);
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

    private class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; init; }
    }
}
