using System.Buffers;
using System.IO.Pipelines;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace TextFile.Parser;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class PipeWorkers(IConfiguration configuration, ILogger<PipeWorkers> logger) : ParserBase(configuration)
{
    private readonly ILogger<PipeWorkers> _logger = logger;

    public override async Task CreateExternalChunks()
    {
        var pipe = new Pipe();
        var writing = FillPipeAsync(InputFile, pipe.Writer);
        var reading = ReadPipeAsync(pipe.Reader, OutputFile);

        await Task.WhenAll(reading, writing);
    }

    public override Task MergeSortedChunks()
    {
        return Task.CompletedTask;
    }

    private static async Task FillPipeAsync(string inputFile, PipeWriter writer)
    {
        const int minimumBufferSize = 512;

        using (var reader = new FileStream(inputFile, FileMode.Open, FileAccess.Read))
        {
            while (true)
            {
                var memory = writer.GetMemory(minimumBufferSize);
                var bytesRead = await reader.ReadAsync(memory);
                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);

                var result = await writer.FlushAsync();
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }

        await writer.CompleteAsync();
    }

    private async Task ReadPipeAsync(PipeReader reader, string outputFile)
    {
        var records = new List<Record>();
        var fileInfo = new FileInfo(InputFile);
        long totalLength = fileInfo.Length;
        long bytesReadSoFar = 0;
        int lastLoggedPercent = 0;

        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            SequencePosition? position;
            do
            {
                position = buffer.PositionOf((byte)'\n');

                if (position != null)
                {
                    var line = buffer.Slice(0, position.Value).ToArray();
                    var lineString = System.Text.Encoding.UTF8.GetString(line);
                    var parts = lineString.Split(new[] { ". " }, 2, StringSplitOptions.TrimEntries);
                    if (parts.Length == 2 && int.TryParse(parts[0], out var number))
                    {
                        records.Add(new Record { Number = number, Text = parts[1] });
                    }

                    buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
                    bytesReadSoFar += line.Length + 1; // +1 for the newline character

                    int percentRead = (int)((bytesReadSoFar / (double)totalLength) * 100);
                    if (percentRead > lastLoggedPercent)
                    {
                        _logger.LogInformation("Read {0}% of the input file", percentRead);
                        lastLoggedPercent = percentRead;
                    }
                }
            } while (position != null);

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await reader.CompleteAsync();
        _logger.LogInformation("Read the input file, starting sort...");

        // Sort records
        records.Sort((x, y) =>
        {
            var textComparison = string.Compare(x.Text, y.Text, StringComparison.Ordinal);
            return textComparison != 0 ? textComparison : x.Number.CompareTo(y.Number);
        });

        _logger.LogInformation("Sorted, starting writing");
        // Write sorted records to output file
        await WriteSortedRecordsToFileAsync(records, outputFile);
        _logger.LogInformation("Done");
    }

    private static async Task WriteSortedRecordsToFileAsync(List<Record> records, string outputFile)
    {
        await using var writer = new StreamWriter(outputFile);
        foreach (var record in records)
        {
            await writer.WriteLineAsync(record.ToString());
        }
    }

    private class Record
    {
        public int Number { get; init; }
        public string Text { get; init; }

        public override string ToString()
        {
            return $"{Number}. {Text}";
        }
    }
}
