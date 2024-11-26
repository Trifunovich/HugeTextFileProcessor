namespace TextFile.Parser;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class Workers2 : ParserBase
{

    public override async Task CreateExternalChunks()
    {
        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 100000);
        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTasks = Enumerable.Range(0, Environment.ProcessorCount).Select(x => Task.Run(() =>
        {
            _procCount[x] = 0;
            return ProcessLinesAsync(x, linesQueue, ChunkFolder);
        })).ToArray();
        await Task.WhenAll(new[] { readingTask }.Concat(processingTasks));
    }

    public override Task MergeSortedChunks()
    {
        var sortedFiles = Directory.GetFiles(Directory.GetCurrentDirectory(), "chunk_*.txt");
        var readers = sortedFiles.Select(file => new StreamReader(file)).ToList();
        var queue = new SortedDictionary<string, QueueItem>();

        foreach (var reader in readers)
        {
            if (reader.Peek() < 0) continue;
            var line = reader.ReadLine();
            if (line != null) queue.Add(line, new QueueItem { Line = line, Reader = reader });
        }

        using (var writer = new StreamWriter(OutputFile))
        {
            while (queue.Count > 0)
            {
                var first = queue.First();
                writer.WriteLine(first.Key);

                var queueItem = first.Value;
                queue.Remove(first.Key);

                if (queueItem.Reader.Peek() >= 0)
                {
                    var line = queueItem.Reader.ReadLine();
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

        return Task.CompletedTask;
    }

    static async Task ReadLinesAsync(string inputFile, BlockingCollection<string> linesQueue)
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
                var lines = new string(buffer, 0, bytesRead).Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
                foreach (var line in lines)
                {
                    linesQueue.Add(line);
                }
            }
        }
        linesQueue.CompleteAdding();
    }

    async Task ProcessLinesAsync(int ind, BlockingCollection<string> linesQueue, string chunkFolder)
    {
        var records = new List<Record>();
        var chunkIndex = 0;
        foreach (var line in linesQueue.GetConsumingEnumerable())
        {
            _procCount[ind]++;
            var parts = line.Split([". "], 2, StringSplitOptions.None);
            if (parts.Length == 2 && int.TryParse(parts[0], out var number))
            {
                records.Add(new Record { Number = number, Text = parts[1] });
            }

            if (records.Count < ChunkSize) continue;
            await WriteChunkToFileAsync(records, chunkIndex++, ind, chunkFolder);
            records.Clear();
        }

        if (records.Count > 0)
        {
            await WriteChunkToFileAsync(records, chunkIndex, ind, chunkFolder);
        }

        Console.WriteLine($"Processor {ind} has count {_procCount[ind]}");
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
            await writer.WriteLineAsync($"{record.Number}. {record.Text}");
        }
    }

    private class Record
    {
        public int Number { get; init; }
        public string Text { get; init; }
    }

    private class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; init; }
    }
}