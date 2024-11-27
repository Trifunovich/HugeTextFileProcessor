using System.Collections.Concurrent;

namespace TextFile.Parser;

public abstract class WorkerParserBase : ParserBase
{
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
                var (key, queueItem) = queue.First();
                await writer.WriteLineAsync(key);

                queue.Remove(key);

                if (queueItem.Reader.Peek() >= 0)
                {
                    var line = await queueItem.Reader.ReadLineAsync();
                    if (line != null) queue.Add(line, new QueueItem { Line = line, Reader = queueItem.Reader });
                }
                else
                {
                    queueItem.Reader.Dispose();
                }
            }
        }

        Directory.Delete(ChunkFolder, true);
    }
    protected async Task ProcessLinesAsync(int ind, BlockingCollection<string> linesQueue, string chunkFolder)
    {
        var records = new List<Record>();
        var chunkIndex = 0;
        foreach (var line in linesQueue.GetConsumingEnumerable())
        {
            _procCount[ind]++;
            var parts = line.Split([". "], 2, StringSplitOptions.TrimEntries);
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

    protected class Record
    {
        public int Number { get; init; }
        public string Text { get; init; }
    }

    protected class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; init; }
    }
}