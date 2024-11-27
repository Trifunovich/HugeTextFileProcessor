using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;

namespace TextFile.Parser;

public class ClassicParser
{

    public async Task ParseAndSortFile(string inputFile)
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDirectory);

        var sortedChunks = await SplitAndSortChunks(inputFile, tempDirectory);

        var timestamp = DateTime.Now
            .ToString("yyyyMMddHHmmss");
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
        var chunkIndex = 0;

        var fileInfo = new FileInfo(inputFile);
        var totalFileSize = fileInfo.Length;
        long processedSize = 0;

        using var mmf = MemoryMappedFile.CreateFromFile(inputFile, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        using var accessor = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

        long position = 0;
        while (position < totalFileSize)
        {
            var line = ReadLine(accessor, ref position);
            if (line == null) break;

            lines.Add(line);
            currentChunkSize += line.Length;
            processedSize += line.Length;

            if (currentChunkSize < 10000) continue;
            var chunkFile = Path.Combine(tempDirectory, $"chunk_{chunkIndex++}.txt");
            await WriteSortedChunk(chunkFile, lines);
            chunks.Add(chunkFile);
            lines.Clear();
            currentChunkSize = 0;
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

    private static string? ReadLine(MemoryMappedViewAccessor accessor, ref long position)
    {
        var buffer = new List<byte>();
        byte b;
        while (position < accessor.Capacity && (b = accessor.ReadByte(position++)) != '\n')
        {
            buffer.Add(b);
        }

        if (buffer.Count == 0) return null;
        return System.Text.Encoding.UTF8.GetString(buffer.ToArray()).TrimEnd('\r');
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