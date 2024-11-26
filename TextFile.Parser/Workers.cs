using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class Workers
{
    private const int ChunkSize = 100000; // Adjust this based on your memory constraints
    private static string InputFile = @"D:\\largefiletext\\input_file_2024112549_1.txt";
    private static string OutputFile = @"D:\\largefiletext\\workers_output.txt";

    [Benchmark]
    public async Task StartBenchmark()
    {
        await Start();
    }



    static async Task Start()
    {

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTask = Task.Run(() => ProcessLinesAsync(linesQueue));

        await Task.WhenAll(readingTask, processingTask);

        MergeSortedChunks(OutputFile);

        Console.WriteLine("File parsing and sorting complete.");
    }

    static async Task ReadLinesAsync(string inputFile, BlockingCollection<string> linesQueue)
    {
        using (var reader = new StreamReader(inputFile))
        {
            string line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                linesQueue.Add(line);
            }
        }
        linesQueue.CompleteAdding();
    }

    static async Task ProcessLinesAsync(BlockingCollection<string> linesQueue)
    {
        var records = new List<Record>();
        int chunkIndex = 0;

        foreach (var line in linesQueue.GetConsumingEnumerable())
        {
            var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);
            if (parts.Length == 2 && int.TryParse(parts[0], out int number))
            {
                records.Add(new Record { Number = number, Text = parts[1] });
            }

            if (records.Count >= ChunkSize)
            {
                await WriteChunkToFileAsync(records, chunkIndex++);
                records.Clear();
            }
        }

        if (records.Count > 0)
        {
            await WriteChunkToFileAsync(records, chunkIndex);
        }
    }

    static async Task WriteChunkToFileAsync(List<Record> records, int chunkIndex)
    {
        records.Sort((x, y) =>
        {
            int textComparison = string.Compare(x.Text, y.Text, StringComparison.Ordinal);
            return textComparison != 0 ? textComparison : x.Number.CompareTo(y.Number);
        });

        string chunkFileName = $"chunk_{chunkIndex}.txt";
        using (var writer = new StreamWriter(chunkFileName))
        {
            foreach (var record in records)
            {
                await writer.WriteLineAsync($"{record.Number}. {record.Text}");
            }
        }
    }

    static void MergeSortedChunks(string outputFile)
    {
        var sortedFiles = Directory.GetFiles(Directory.GetCurrentDirectory(), "chunk_*.txt");
        var readers = sortedFiles.Select(file => new StreamReader(file)).ToList();
        var queue = new SortedDictionary<string, QueueItem>();

        foreach (var reader in readers)
        {
            if (reader.Peek() >= 0)
            {
                var line = reader.ReadLine();
                queue.Add(line, new QueueItem { Line = line, Reader = reader });
            }
        }

        using (var writer = new StreamWriter(outputFile))
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
    }

    class Record
    {
        public int Number { get; set; }
        public string Text { get; set; }
    }

    class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; set; }
    }
}