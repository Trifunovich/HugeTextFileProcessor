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
    protected string[] _chunkFolders;

    public IEnumerable<string> InputFiles()
    {
        return _textFiles;
    }

    [GlobalSetup]
    public virtual void Setup()
    {
        Timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
        ChunkFolder = $@"F:\\largetextfiles\\chunks_bench_{Timestamp}";
        OutputFileFolder = $@"F:\\largetextfiles\\output_bench_{Timestamp}";
    }

    public string Timestamp { get; set; }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.bench.json", optional: false, reloadOnChange: true);
            });
}
