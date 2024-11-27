using BenchmarkDotNet.Loggers;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;

namespace TextFile.Parser;

public class ClassicParser : WorkerParserBase
{
    public override async Task CreateExternalChunks()
    {
        await base.CreateExternalChunks();
        _ = await SplitAndSortChunks(InputFile, ChunkFolder);
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

            if (currentChunkSize < ChunkSize) continue;
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
            .OrderBy(line => line.Split(". ")[1].Trim())
            .ThenBy(line => int.Parse(line.Split(". ")[0]));
        await File.WriteAllLinesAsync(chunkFile, sortedLines);
    }
}