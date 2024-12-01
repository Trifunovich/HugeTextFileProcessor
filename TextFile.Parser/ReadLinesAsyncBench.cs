using System.Buffers;
using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public class ReadLinesAsyncBench : BenchBase
{
    [Params(10000)]
    public int N { get; set; }
   

    [Benchmark(Baseline = true)]
    [ArgumentsSource(nameof(InputFiles))]
    public async Task ReadLinesAsync_Benchmark(string inputFile)
    {
        var linesQueue = new BlockingCollection<(int, string)>();

        await ReadLinesAsync(inputFile, linesQueue, N);
    }

    [Benchmark]
    [ArgumentsSource(nameof(InputFiles))]
    public async Task ReadLinesAsyncSpan_Benchmark(string inputFile)
    {
        var linesQueue = new BlockingCollection<(int, string)>();

        await ReadLinesInParallelAsync(inputFile, linesQueue);
    }


    private async Task ReadLinesInParallelAsync(string inputFile, BlockingCollection<(int, string)> linesQueue)
    {
        const int numberOfReaders = 2; // Read from both ends
        var fileInfo = new FileInfo(inputFile);
        var fileSize = fileInfo.Length;
        var tasks = new Task[numberOfReaders];

        // Calculate chunk sizes and positions
        var chunkSize = fileSize / numberOfReaders;
        var positions = new (long Start, long End)[numberOfReaders];

        for (int i = 0; i < numberOfReaders; i++)
        {
            positions[i].Start = i * chunkSize;
            positions[i].End = (i == numberOfReaders - 1) ? fileSize : (i + 1) * chunkSize;
        }

        // Start tasks to read each chunk
        for (int i = 0; i < numberOfReaders; i++)
        {
            var start = positions[i].Start;
            var end = positions[i].End;
            tasks[i] = Task.Run(() => ReadFileChunkAsync(inputFile, start, end, linesQueue));
        }

        await Task.WhenAll(tasks);
        linesQueue.CompleteAdding();
    }

    private async Task ReadFileChunkAsync(string inputFile, long startPosition, long endPosition, BlockingCollection<(int, string)> linesQueue)
    {
        await using var fs = new FileStream(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var reader = new StreamReader(fs);

        // Adjust the start position to the beginning of the next full line
        fs.Seek(startPosition, SeekOrigin.Begin);
        if (startPosition != 0)
        {
            // Discard partial line if not at the start of the file
            await reader.ReadLineAsync();
        }

        while (fs.Position < endPosition && await reader.ReadLineAsync() is { } line)
        {
            var parts = line.Split([". "], 2, StringSplitOptions.TrimEntries);
            if (parts.Length == 2 && int.TryParse(parts[0], out var number))
            {
                linesQueue.Add((number, parts[1]));
            }

            if (fs.Position >= endPosition)
            {
                break;
            }
        }
    }

    private static async Task ReadLinesAsync(string? inputFile, BlockingCollection<(int, string)> linesQueue, int bulkWriteSize)
    {
        if (!File.Exists(inputFile))
        {
            throw new FileNotFoundException($"The file {inputFile} does not exist.");
        }

        using (var reader = new StreamReader(inputFile))
        {
            var buffer = ArrayPool<char>.Shared.Rent(bulkWriteSize);
            try
            {
                int bytesRead;
                while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    var lines = new string(buffer, 0, bytesRead).Split(["\r\n", "\n"], StringSplitOptions.TrimEntries);
                    Parallel.ForEach(lines, line =>
                    {
                        var parts = line.Split([". "], 2, StringSplitOptions.TrimEntries);
                        if (parts.Length == 2 && int.TryParse(parts[0], out var number))
                        {
                            linesQueue.Add((number, parts[1]));
                        }
                    });
                }
            }
            finally
            {
                ArrayPool<char>.Shared.Return(buffer);
            }
        }

        linesQueue.CompleteAdding();
    }

    private static async Task ReadLinesAsyncSpan(string inputFile, BlockingCollection<(int, string)> linesQueue, int bulkWriteSize)
    {
        using (var reader = new StreamReader(inputFile))
        {
            var buffer = ArrayPool<char>.Shared.Rent(bulkWriteSize);
            try
            {
                int bytesRead;
                while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    var span = buffer.AsSpan(0, bytesRead);
                    var lines = span.ToString().Split(["\r\n", "\n"], StringSplitOptions.TrimEntries);
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
            finally
            {
                ArrayPool<char>.Shared.Return(buffer);
            }
        }

        linesQueue.CompleteAdding();
    }
}
