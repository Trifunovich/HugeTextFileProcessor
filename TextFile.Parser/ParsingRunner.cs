using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public class ParsingRunner
{
    //due to benchmarks being static, we can't use DI here
    private static IParser _parser;

    public static void SetParser(IParser parser)
    {
        _parser = parser;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public static async Task Start()
    {
        await Run(_parser);
    }

    private static async Task Run(IParser parser)
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
