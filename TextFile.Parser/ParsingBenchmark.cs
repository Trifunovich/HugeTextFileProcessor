using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public class ParsingBenchmark
{
    [Benchmark(OperationsPerInvoke = 2)]
    public async Task BenchmarkParseAndSortFile()
    {
        var parser = new ParallelBackgroundWorkers();
        await Run(parser);
    }

    public static async Task Run(IParser parser)
    {
        var startTime = DateTime.Now;
        Console.WriteLine($"[0.000] Starting to parse and create chunks");
        await parser.CreateExternalChunks();
        var midTime = DateTime.Now;
        Console.WriteLine($"[{(midTime - startTime).TotalSeconds:F3}] Starting to merge output file");
        await parser.MergeSortedChunks();
        var endTime = DateTime.Now;
        Console.WriteLine($"[{(endTime - startTime).TotalSeconds:F3}] Finished processing");
    }
}
