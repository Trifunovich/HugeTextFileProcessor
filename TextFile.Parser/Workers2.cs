namespace TextFile.Parser;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class Workers2 : WorkerParserBase
{
    public override async Task CreateExternalChunks()
    {
        await base.CreateExternalChunks();
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 100000);
        var readingTask = Task.Run(() => ReadLinesAsync(InputFile, linesQueue));
        var processingTasks = Enumerable.Range(0, Environment.ProcessorCount).Select(x => Task.Run(() =>
        {
            _procCount[x] = 0;
            return ProcessLinesAsync(x, linesQueue, ChunkFolder);
        })).ToArray();
        await Task.WhenAll(new[] { readingTask }.Concat(processingTasks));
    }

    static async Task ReadLinesAsync(string inputFile, BlockingCollection<string> linesQueue)
    {
        if (!File.Exists(inputFile))
        {
            throw new FileNotFoundException($"The file {inputFile} does not exist.");
        }

        using (var reader = new StreamReader(inputFile))
        {
            var buffer = new char[ChunkSize];
            int bytesRead;
            while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                var lines = new string(buffer, 0, bytesRead).Split(["\r\n", "\n"], StringSplitOptions.None);
                foreach (var line in lines)
                {
                    linesQueue.Add(line);
                }
            }
        }
        linesQueue.CompleteAdding();
    }
}