using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public class ParsingBenchmark
{
    private readonly FileParser _parser = new FileParser();

    [Benchmark]
    public async Task BenchmarkParseAndSortFile()
    {
        var parser = new Workers2();
        await Run(parser);
    }

    public static async Task Run(IParser parser)
    {
        

        var startTime = DateTime.Now;
        Console.WriteLine($"[0.000] Starting to parse and create chunks");
        await parser.CreateExternalChunks(inputFile, chunkFolder);
        var midTime = DateTime.Now;
        Console.WriteLine($"[{(midTime - startTime).TotalSeconds:F3}] Starting to merge output file");
        await parser.MergeSortedChunks(outputFile);
        var endTime = DateTime.Now;
        Console.WriteLine($"[{(endTime - startTime).TotalSeconds:F3}] Finished processing");
    }
}
