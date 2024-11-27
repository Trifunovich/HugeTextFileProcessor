using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class Workers : ParserBase
{

    public override async Task CreateExternalChunks()
    {
        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTask = Task.Run(() => ProcessLinesAsync(linesQueue));

        await Task.WhenAll(readingTask, processingTask);
    }

    private static async Task ReadLinesAsync(string inputFile, BlockingCollection<string> linesQueue)
    {
        using (var reader = new StreamReader(inputFile))
        {
            while (await reader.ReadLineAsync() is { } line)
            {
                linesQueue.Add(line);
            }
        }
        linesQueue.CompleteAdding();
    }

    private static async Task ProcessLinesAsync(BlockingCollection<string> linesQueue)
    {
        var records = new List<Record>();
        var chunkIndex = 0;

        foreach (var line in linesQueue.GetConsumingEnumerable())
        {
            var parts = line.Split([". "], 2, StringSplitOptions.None);
            if (parts.Length == 2 && int.TryParse(parts[0], out var number))
            {
                records.Add(new Record { Number = number, Text = parts[1] });
            }

            if (records.Count < ChunkSize) continue;
            await WriteChunkToFileAsync(records, chunkIndex++);
            records.Clear();
        }

        if (records.Count > 0)
        {
            await WriteChunkToFileAsync(records, chunkIndex);
        }
    }

    private static async Task WriteChunkToFileAsync(List<Record> records, int chunkIndex)
    {
        records.Sort((x, y) =>
        {
            var textComparison = string.Compare(x.Text, y.Text, StringComparison.Ordinal);
            return textComparison != 0 ? textComparison : x.Number.CompareTo(y.Number);
        });

        var chunkFileName = $"chunk_{chunkIndex}.txt";
        await using var writer = new StreamWriter(chunkFileName);
        foreach (var record in records)
        {
            await writer.WriteLineAsync($"{record.Number}. {record.Text}");
        }
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

    private class Record
    {
        public int Number { get; set; }
        public string Text { get; set; }
    }

    private class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; set; }
    }
}