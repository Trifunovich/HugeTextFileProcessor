namespace TextFile.Parser;

public interface IParser
{
    Task CreateExternalChunks();

    Task MergeSortedChunks();
}