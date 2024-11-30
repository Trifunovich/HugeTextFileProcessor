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

        await ReadLinesAsyncSpan(inputFile, linesQueue, N);
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
