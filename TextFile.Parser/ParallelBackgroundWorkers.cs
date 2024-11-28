using Microsoft.Extensions.Configuration;

namespace TextFile.Parser;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class ParallelBackgroundWorkers(IConfiguration configuration) : ParserBase(configuration)
{
    public override async Task CreateExternalChunks()
    {
        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<(int, string)>(boundedCapacity: BoundedCap);
        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTasks = Enumerable.Range(0, Environment.ProcessorCount).Select(x => Task.Run(() =>
        {
            ProcCount[x] = 0;
            return ProcessLinesAsync(x, linesQueue, ChunkFolder);
        })).ToArray();
        await Task.WhenAll(new[] { readingTask }.Concat(processingTasks));
    }

    public override async Task MergeSortedChunks()
    {
        var sortedFiles = Directory.GetFiles(ChunkFolder, "chunk_*.txt");
        var readers = sortedFiles.Select(file => new StreamReader(file)).ToList();
        var queue = new SortedDictionary<string, QueueItem>();

        foreach (var reader in readers.Where(reader => reader.Peek() >= 0))
        {
            var line = await reader.ReadLineAsync();
            if (line != null) queue.Add(line, new QueueItem { Line = line, Reader = reader });
        }

        await using (var writer = new StreamWriter(OutputFile))
        {
            while (queue.Count > 0)
            {
                var first = queue.First();
                await writer.WriteLineAsync(first.Key);

                var queueItem = first.Value;
                queue.Remove(first.Key);

                if (queueItem.Reader.Peek() >= 0)
                {
                    var line = await queueItem.Reader.ReadLineAsync();
                    queue.Add(line, new QueueItem { Line = line, Reader = queueItem.Reader });
                }
                else
                {
                    queueItem.Reader.Dispose();
                }
            }
        }

        foreach (var file in sortedFiles)
        {
            File.Delete(file);
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
            var buffer = new char[ChunkSize];
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

    private async Task ProcessLinesAsync(int ind, BlockingCollection<(int, string)> linesQueue, string chunkFolder)
    {
        var records = new List<Record>();
        var chunkIndex = 0;
        foreach (var line in linesQueue.GetConsumingEnumerable())
        {
            ProcCount[ind]++;
            records.Add(new() {Number = line.Item1, Text = line.Item2});

            if (records.Count < ChunkSize) continue;
            await WriteChunkToFileAsync(records, chunkIndex++, ind, chunkFolder);
            records.Clear();
        }

        if (records.Count > 0)
        {
            await WriteChunkToFileAsync(records, chunkIndex, ind, chunkFolder);
        }

        Console.WriteLine($"Processor {ind} has count {ProcCount[ind]}");
    }

    private static async Task WriteChunkToFileAsync(List<Record> records, int chunkIndex, int ind, string chunkFolder)
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
