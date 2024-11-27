namespace TextFile.Parser;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

public class Workers : WorkerParserBase
{

    public override async Task CreateExternalChunks()
    {
        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        _procCount[0] = 0;
        var processingTask = Task.Run(() => ProcessLinesAsync(0, linesQueue, ChunkFolder));

        await Task.WhenAll(readingTask, processingTask);
    }

    private static async Task ReadLinesAsync(string inputFile, BlockingCollection<string> linesQueue)
    {
        using (var reader = new StreamReader(inputFile))
        {
            while (await reader.ReadLineAsync() is { } line)
            {
                linesQueue.Add(line);
            }
        }
        linesQueue.CompleteAdding();
    }
}