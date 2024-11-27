using System.Collections.Concurrent;

namespace TextFile.Parser;

public abstract class ParserBase : IParser
{
    protected const int ChunkSize = 10000000; // Adjust this based on your memory constraints
    protected readonly ConcurrentDictionary<int, long> _procCount = new();
    protected static string? CurrentTs;
    protected string ChunkFolder;
    protected string InputFile;
    protected string OutputFile;

    protected ParserBase()
    {
        InputFile = "D:\\largefiletext\\input_file_2024112549_1.txt";
        var outputFolder = Path.GetDirectoryName(InputFile);
        CurrentTs ??= DateTime.Now.ToString("yyyyMMddHHmmss");
        OutputFile = $"{outputFolder}\\output_{GetType().Name}_{CurrentTs}.txt";
        ChunkFolder = $"{outputFolder}\\chunks_{CurrentTs}";
    }


    public void SetPaths(string inputFile, string outputFile, string chunkFolder)
    {
        InputFile = inputFile;
        OutputFile = outputFile;
        ChunkFolder = chunkFolder;
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