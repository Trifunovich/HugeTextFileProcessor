using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace TextFile.Parser;

public abstract class ParserBase : IParser
{
    protected static int ChunkSize = 1_000_000;
    protected static int BulkWriteSize = 100000;
    protected static int BoundedCap = 10000;
    protected readonly ConcurrentDictionary<int, long> ProcCount = new();
    protected readonly ConcurrentDictionary<int, long> MmCount = new();
    protected static string? CurrentTs;
    protected string ChunkFolder;
    protected string? InputFile;
    protected string OutputFile;

    protected ParserBase(IConfiguration configuration)
    {
        var section = configuration.GetSection("ParserSettings");
        InputFile = section.GetValue<string>("InputFilePath");
        ChunkSize = section.GetValue<int>("ChunkSize");
        BoundedCap = section.GetValue<int>("BoundedCap");
        var configOutputFold = section.GetValue<string>("OutputFolder");
        var outputFolder = string.IsNullOrEmpty(configOutputFold) 
                            || configOutputFold.Equals("*") 
                            || configOutputFold.Equals("auto")
            ? Path.GetDirectoryName(InputFile) : configOutputFold;
        CurrentTs ??= DateTime.Now.ToString("yyyyMMddHHmmss");
        OutputFile = $"{outputFolder}\\output_{GetType().Name}_{CurrentTs}.txt";
        ChunkFolder = $"{outputFolder}\\chunks_{CurrentTs}";
    }

    public virtual Task CreateExternalChunks()
    {
        if (Directory.Exists(ChunkFolder))
        {
            var directoryInfo = new DirectoryInfo(ChunkFolder);
            foreach (var file in directoryInfo.GetFiles())
            {
                file.Delete();
            }
            foreach (var dir in directoryInfo.GetDirectories())
            {
                dir.Delete(true);
            }
        }
        else
        {
            Directory.CreateDirectory(ChunkFolder);
        }

        return Task.CompletedTask;
    }


    public abstract Task MergeSortedChunks();
}