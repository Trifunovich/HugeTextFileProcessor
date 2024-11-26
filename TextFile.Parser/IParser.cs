using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public interface IParser
{
    [Benchmark]
    Task CreateExternalChunks(string inputFile);

    [Benchmark]
    Task MergeSortedChunks(string outputFile);
}