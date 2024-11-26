using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public class ParsingBenchmark
{
    private readonly FileParser _parser = new FileParser();

    [Benchmark]
    public async Task BenchmarkParseAndSortFile()
    {
        const string inputFile = "D:\\largefiletext\\input_file_2024112517_10.txt";
        var outputFolder = Path.GetDirectoryName(inputFile);
        var parser = new Workers2();
        var outputFile = $"{outputFolder}\\output_{parser.GetType().Name}_{DateTime.Now.ToString("yyyyMMddHHmmss")}.txt";

        await Run(parser, inputFile, outputFile);
    }

    public static async Task Run(IParser parser, string inputFile, string outputFile)
    {
        var startTime = DateTime.Now;
        Console.WriteLine($"[0.000] Starting to parse and create chunks");
        await parser.CreateExternalChunks(inputFile);
        var midTime = DateTime.Now;
        Console.WriteLine($"[{(midTime - startTime).TotalSeconds:F3}] Starting to merge output file");
        await parser.MergeSortedChunks(outputFile);
        var endTime = DateTime.Now;
        Console.WriteLine($"[{(endTime - startTime).TotalSeconds:F3}] Finished processing");
    }
}
