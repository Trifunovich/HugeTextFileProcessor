using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.Collections.Concurrent;
using System.Linq;

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

        var inputFile = args[0];
        var parser = new FileParser();
        await parser.ParseAndSortFile(inputFile);

        BenchmarkRunner.Run<ParsingBenchmark>();
    }
}

public class FileParser
{
    private const long MaxMemoryUsage = 8L * 1024 * 1024 * 1024; // 8GB
    private const long MaxChunkSize = MaxMemoryUsage / 2; // 4GB to leave room for sorting

    public async Task ParseAndSortFile(string inputFile)
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDirectory);

        var sortedChunks = await SplitAndSortChunks(inputFile, tempDirectory);

        var timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
        var inputFileWoExt = Path.GetFileNameWithoutExtension(inputFile);
        var extension = Path.GetExtension(inputFile);
        var outputFile = $"{inputFileWoExt}_output_{timestamp}{extension}";

        Console.WriteLine($"Writing to {outputFile}...");
        await MergeSortedChunks(sortedChunks, outputFile);
        Console.WriteLine($"Finished, check {outputFile}");

        Directory.Delete(tempDirectory, true);
    }

    private async Task<List<string>> SplitAndSortChunks(string inputFile, string tempDirectory)
    {
        var chunks = new ConcurrentBag<string>();
        var lines = new List<string>();
        long currentChunkSize = 0;
        int chunkIndex = 0;

        var fileInfo = new FileInfo(inputFile);
        long totalFileSize = fileInfo.Length;
        long processedSize = 0;

        using var reader = new StreamReader(File.OpenRead(inputFile), bufferSize: 1024 * 1024); // 1MB buffer

        while (await reader.ReadLineAsync() is { } line)
        {
            lines.Add(line);
            currentChunkSize += line.Length;
            processedSize += line.Length;

            if (currentChunkSize >= MaxChunkSize)
            {
                var chunkFile = Path.Combine(tempDirectory, $"chunk_{chunkIndex++}.txt");
                await WriteSortedChunk(chunkFile, lines);
                chunks.Add(chunkFile);
                lines.Clear();
                currentChunkSize = 0;
            }

            // Report progress
            Console.Write($"\rProgress: {processedSize * 100.0 / totalFileSize:F3}%");
        }

        if (lines.Count > 0)
        {
            var chunkFile = Path.Combine(tempDirectory, $"chunk_{chunkIndex++}.txt");
            await WriteSortedChunk(chunkFile, lines);
            chunks.Add(chunkFile);
        }

        Console.WriteLine("\nSplitting and sorting completed.");
        return chunks.ToList();
    }

    private static async Task WriteSortedChunk(string chunkFile, List<string> lines)
    {
        var sortedLines = lines
            .AsParallel()
            .OrderBy(line => line.Split(". ")[1])
            .ThenBy(line => int.Parse(line.Split(". ")[0]));
        await File.WriteAllLinesAsync(chunkFile, sortedLines);
    }

    private static async Task MergeSortedChunks(List<string> sortedChunks, string outputFile)
    {
        var readers = sortedChunks.Select(chunk => new StreamReader(File.OpenRead(chunk), bufferSize: 1024 * 1024)).ToList(); // 1MB buffer
        var queue = new SortedList<string, StreamReader>();

        foreach (var reader in readers)
        {
            if (await reader.ReadLineAsync() is string line)
            {
                queue.Add(line, reader);
            }
        }

        await using (var writer = new StreamWriter(File.OpenWrite(outputFile), bufferSize: 1024 * 1024)) // 1MB buffer
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
        await _parser.ParseAndSortFile(@"D:\\largefiletext\\input_file_2024112549_1.txt"); // Adjust with your actual file path, can be in a config or whatever
    }
}
