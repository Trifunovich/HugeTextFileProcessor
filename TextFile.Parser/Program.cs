using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace TextFile.Parser;

internal class Program
{

    private static async Task Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: <program> <input_file> <chunk_size>");
            return;
        }

        var inputFile = args[0];

        var parsed = int.TryParse(args[1], out int parsedV);

        if (!parsed)
        {
            Console.WriteLine("Invalid chunk size");
            return;
        }

        var chunkSize = parsedV;
        var parser = new FileParser();
        await parser.ParseAndSortFile(inputFile, chunkSize);

        BenchmarkRunner.Run<ParsingBenchmark>();
    }
}

public class FileParser
{
    public async Task ParseAndSortFile(string inputFile, int chunkSize)
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDirectory);

        var sortedChunks = await SplitAndSortChunks(inputFile, tempDirectory, chunkSize);

        var timestamp = DateTime.Now.ToString("yyyymmddss");
        var inputFileWoExt = Path.GetFileNameWithoutExtension(inputFile);
        var extension = Path.GetExtension(inputFile);
        var outputFile = $"{inputFileWoExt}_output_{timestamp}{extension}";
        
        Console.WriteLine($"Writing to {outputFile}...");
        await MergeSortedChunks(sortedChunks, outputFile);
        Console.WriteLine($"Finished, check {outputFile}");

        Directory.Delete(tempDirectory, true);
    }

    private async Task<List<string>> SplitAndSortChunks(string inputFile, string tempDirectory, int chunkSize)
    {
        // Adjust based on memory constraints
        var chunks = new List<string>();
        var lines = new List<string>(chunkSize);

        using var reader = new StreamReader(inputFile);
        long currentLines = 0;

        while (await reader.ReadLineAsync() is { } line)
        {
            lines.Add(line);
            currentLines++;

            if (lines.Count >= chunkSize)
            {
                var chunkFile = Path.Combine(tempDirectory, $"chunk_{chunks.Count}.txt");
                await WriteSortedChunk(chunkFile, lines);
                chunks.Add(chunkFile);
                lines.Clear();
            }

            var progress = currentLines;
            Console.Write($"\r{currentLines:d} lines parsed");
        }

        if (lines.Count <= 0) return chunks;
        {
            var chunkFile = Path.Combine(tempDirectory, $"chunk_{chunks.Count}.txt");
            await WriteSortedChunk(chunkFile, lines);
            chunks.Add(chunkFile);
        }

        return chunks;
    }

    private static async Task WriteSortedChunk(string chunkFile, List<string> lines)
    {
        var sortedLines = lines
            .OrderBy(line => line.Split(". ")[1])
            .ThenBy(line => int.Parse(line.Split(". ")[0]));
        await File.WriteAllLinesAsync(chunkFile, sortedLines);
    }

    private static async Task MergeSortedChunks(List<string> sortedChunks, string outputFile)
    {
        var readers = 
            sortedChunks.Select(chunk => new StreamReader(chunk)).ToList();
        var queue = new SortedList<string, StreamReader>();

        foreach (var reader in readers)
        {
            if (await reader.ReadLineAsync() is string line)
            {
                queue.Add(line, reader);
            }
        }

        await using (var writer = new StreamWriter(outputFile))
        {
            while (queue.Any())
            {
                var kvp = queue.First();
                queue.RemoveAt(0);

                await writer.WriteLineAsync(kvp.Key);

                if (await kvp.Value.ReadLineAsync() is string line)
                {
                    queue.Add(line, kvp.Value);
                }
            }
        }

        foreach (var reader in readers)
        {
            reader.Dispose();
        }
    }
}

public class ParsingBenchmark
{
    private readonly FileParser _parser = new FileParser();

    [Benchmark]
    public async Task BenchmarkParseAndSortFile()
    {
        await _parser.ParseAndSortFile(@"D:\\largefiletext\\input_file_2024112549_1.txt", 10000); // Adjust with your actual file path, can be in a config or whatever
    }
}