using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Loggers;
using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Serilog.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace TextFile.Parser;

public class MicroMergeAsyncBench : BenchBase
{
    private static int mmIndex = 0;
    protected static int ChunkSize = 1_000_000;
    protected readonly ConcurrentDictionary<int, long> ProcCount = new();
    protected readonly ConcurrentDictionary<int, long> MmCount = new();

    [GlobalSetup]
    public override void Setup()
    {
        base.Setup();

        var host = CreateHostBuilder([]).Build();
        var c = host.Services.GetRequiredService<IConfiguration>();
        var chFolders = c.GetSection("ParserSettings").GetValue<string[]>("ChunkFolders");
        if (chFolders == null) return;
        _chunkFolders = chFolders.Select(f => $"{f}_{Timestamp}").ToArray();
        foreach (var sourceFolder in chFolders)
        {
            foreach (var destFolder in _chunkFolders)
            {
                foreach (var file in Directory.GetFiles(sourceFolder))
                {
                    var destFile = Path.Combine(destFolder, Path.GetFileName(file));
                    Directory.CreateDirectory(destFolder);
                    File.Copy(file, destFile, true);
                }
            }
        }
    }

    [Benchmark(Baseline = true)]
    [ArgumentsSource(nameof(InputChunkFiles))]
    public async Task MicroMergeAsync_Benchmark((IEnumerable<string>, string) inputFiles)
    {
        var filesQueue = new BlockingCollection<string>();

        foreach (var f in inputFiles.Item1)
        {
            filesQueue.Add(f);
        }

        ProcCount[0] = 0;
        var nullLogger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        await MicroMergeAsync(filesQueue, 0, nullLogger, Path.Combine(OutputFileFolder, inputFiles.Item2));
    }

    public IEnumerable<(IEnumerable<string>, string)> InputChunkFiles()
    {
        return _chunkFolders.Select(folder =>
            new ValueTuple<IEnumerable<string>, string>(
                Directory.GetFiles(folder).AsEnumerable(),
                folder
            )
        );
    }

    private static readonly Lock FileLock = new();

    private async Task MicroMergeAsync(BlockingCollection<string> filesQueue, int ind, Microsoft.Extensions.Logging.ILogger logger, string outputFile)
    {
        while (true)
        {
            while (filesQueue.Count < 2)
            {
                if (!filesQueue.IsAddingCompleted)
                {
                    await Task.Delay(100);
                }
                else
                {
                    logger.LogInformation("Final merge will be done later");
                    logger.LogInformation("Task {Index} has processed {Qty} lines", ind, MmCount[ind]);
                    return;
                }
            }

            string file1, file2;
            lock (FileLock)
            {
                if (filesQueue.Count < 2)
                {
                    continue;
                }
                file1 = filesQueue.Take();
                file2 = filesQueue.Take();
            }

            var newFileName = GenerateNewFileName(file1, mmIndex++);

            if (file2 != null)
            {
                await MergeFiles([file1, file2], newFileName, logger);
            }
            else
            {
                File.Move(file1, outputFile);
                return;
            }

            if (!filesQueue.IsAddingCompleted)
            {
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
