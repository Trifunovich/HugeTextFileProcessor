using BenchmarkDotNet.Running;

namespace TextFile.Parser;

internal class Program
{
    private static async Task Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: <program> <input_file>");
            return;
        }

        var inputFile = args[0] ?? "D:\\largefiletext\\input_file_2024112517_10.txt";
        var outputFolder = Path.GetDirectoryName(inputFile);
        var parser = new ParallelBackgroundWorkers();
        var outputFile = $"{outputFolder}\\output_{parser.GetType().Name}_{DateTime.Now.ToString("yyyyMMddHHmmss")}.txt";

        //await (new ParsingBenchmark()).Run(parser, inputFile, outputFile);
        BenchmarkRunner.Run<ParsingBenchmark>();
        Console.WriteLine("File parsing and sorting complete.");
    }
}