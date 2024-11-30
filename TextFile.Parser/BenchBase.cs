using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace TextFile.Parser;

[MemoryDiagnoser]
public abstract class BenchBase
{
    private readonly string[] _textFiles =
    [
        @"F:\\largetextfiles\\bench_100.txt",
        @"F:\\largetextfiles\\bench_10000.txt",
        @"F:\\largetextfiles\\bench_1000000.txt"
    ];

    protected string ChunkFolder;
    protected string OutputFileFolder;

    public IEnumerable<string> InputFiles()
    {
        return _textFiles;
    }

    protected BenchBase()
    {
        Timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
    }

    [GlobalSetup]
    public virtual void Setup()
    {
        ChunkFolder = $@"F:\\largetextfiles\\chunks_bench_{Timestamp}";
        OutputFileFolder = $@"F:\\largetextfiles\\output_bench_{Timestamp}";
        if (!Directory.Exists(OutputFileFolder))
        {
            Directory.CreateDirectory(OutputFileFolder);
        }
    }

    protected string? Timestamp { get; set; }
}
