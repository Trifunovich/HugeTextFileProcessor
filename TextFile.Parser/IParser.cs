using BenchmarkDotNet.Attributes;

namespace TextFile.Parser;

public interface IParser
{
    [Benchmark]
    Task CreateExternalChunks();

    [Benchmark]
    Task MergeSortedChunks();


}