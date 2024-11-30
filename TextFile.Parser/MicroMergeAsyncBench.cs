using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace TextFile.Parser;

public class MicroMergeAsyncBench : BenchBase
{
    private static int mmIndex = 0;
    protected readonly ConcurrentDictionary<int, long> ProcCount = new();
    protected readonly ConcurrentDictionary<int, long> MmCount = new();

    protected string[] _chunkFolders;

    protected readonly string[] _generatedChunkFolders =
    [
        "F:\\largetextfiles\\small_chunk_folder",
        "F:\\largetextfiles\\medium_chunk_folder",
        "F:\\largetextfiles\\large_chunk_folder"
    ];

    [Benchmark(Baseline = true)]
    public async Task MicroMergeAsyncL_Benchmark()
    {
        await RunMicroMerge("large");
    }

    [Benchmark]
    public async Task MicroMergeAsyncM_Benchmark()
    {
        await RunMicroMerge("medium");
    }

    [Benchmark]
    public async Task MicroMergeAsyncS_Benchmark()
    {
        await RunMicroMerge("small");
    }

    public override void Setup()
    {
        base.Setup();
        _chunkFolders = _generatedChunkFolders.Select(f => $"{f}_{Timestamp}").ToArray();
    }

    private async Task RunMicroMerge(string size)
    {
        var genFol = _generatedChunkFolders.First(x => x.Contains(size));
        var chFol = _chunkFolders.First(x => x.Contains(size));

        if (Directory.Exists(chFol))
        {
            Directory.Delete(chFol, true);
        }

        var outputPath = Path.Combine(OutputFileFolder, size);

        if (Directory.Exists(OutputFileFolder))
        {
            Directory.Delete(OutputFileFolder, true);
            Directory.CreateDirectory(OutputFileFolder);
        }

        foreach (var file in Directory.GetFiles(genFol))
        {
            var destFile = Path.Combine(chFol, Path.GetFileName(file));
            Directory.CreateDirectory(chFol);
            File.Copy(file, destFile, true);
        }

        var filesQueue = new BlockingCollection<string>();

        foreach (var f in Directory.GetFiles(chFol))
        {
            filesQueue.Add(f);
        }


        ProcCount[0] = 0;
        var nullLogger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        await MicroMergeAsync(filesQueue, 0, nullLogger, outputPath);
    }


    private static readonly Lock FileLock = new();

    private async Task MicroMergeAsync(BlockingCollection<string> filesQueue, int ind, Microsoft.Extensions.Logging.ILogger logger, string outputFile)
    {
        while (true)
        {
            if (filesQueue.Count < 2)
            {
                filesQueue.CompleteAdding();
                File.Move(filesQueue.Take(), outputFile);
                break;
            }

            string file1, file2;
            lock (FileLock)
            {
                file1 = filesQueue.Take();
                file2 = filesQueue.Take();
            }

            var newFileName = GenerateNewFileName(file1, mmIndex++);

            if (file2 != null)
            {
                await MergeFiles([file1, file2], newFileName, logger);
                filesQueue.Add(newFileName);
            }

            logger.LogDebug("File {FilePath} merged out of {Fl1} and {Fl2}", newFileName, file1, file2);
        }
    }

    private async Task MergeFiles(string[] sortedFiles, string outputFile, Microsoft.Extensions.Logging.ILogger logger)
    {
        try
        {
            var readers = sortedFiles.Select(file => new StreamReader(file)).ToList();
            var priorityQueue = new PriorityQueue<QueueItem, string>();

            foreach (var reader in readers.Where(reader => reader.Peek() >= 0))
            {
                var line = await reader.ReadLineAsync();
                if (line != null)
                {
                    priorityQueue.Enqueue(new QueueItem { Line = line, Reader = reader }, line);
                }
            }

            await using (var fileStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write))
            await using (var bufferedWriter = new BufferedStream(fileStream))
            await using (var writer = new StreamWriter(bufferedWriter))
            {
                while (priorityQueue.Count > 0)
                {
                    var value = priorityQueue.Dequeue();
                    await writer.WriteLineAsync(value.Line);

                    if (value.Reader.Peek() >= 0)
                    {
                        var line = await value.Reader.ReadLineAsync();
                        if (line != null)
                        {
                            priorityQueue.Enqueue(new QueueItem { Line = line, Reader = value.Reader }, line);
                        }
                    }
                    else
                    {
                        value.Reader.Dispose();
                    }
                }
            }

            foreach (var file in sortedFiles)
            {
                File.Delete(file);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error while merging files");
        }
    }

    private string GenerateNewFileName(string originalFileName, int index)
    {
        var lastChunkIndex = originalFileName.LastIndexOf("chunk", StringComparison.Ordinal);
        var baseName = originalFileName.Substring(0, lastChunkIndex + "chunk".Length);
        return $"{baseName}_mm{index}.txt";
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var folder in _chunkFolders)
        {
            if (Directory.Exists(folder))
            {
                Directory.Delete(folder, true);
            }
        }
    }

    private class QueueItem
    {
        public string Line { get; set; }
        public StreamReader Reader { get; set; }
    }
}
