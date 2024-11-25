using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

internal class Program
{
    private static readonly Random Random = new Random();
    private static readonly char[] Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ".ToCharArray();
    private static readonly List<string> GeneratedStrings = new List<string>();
    private const int MaxStoredStrings = 10000; // Limit the number of stored strings

    private const int BarWidth = 50;
    private static double _sizeInGb = 0;

    static async Task Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("Usage: <size_in_gb> <output_folder_path> <repetition_ratio>");
            return;
        }

        if (!double.TryParse(args[0], out var sizeG))
        {
            Console.WriteLine("Invalid size in GB");
            return;
        }

        var outputFolderPath = args[1];

        if (!double.TryParse(args[2], out var repetitionRatio) || repetitionRatio < 0 || repetitionRatio > 1)
        {
            Console.WriteLine("Invalid repetition ratio. It should be between 0 and 1.");
            return;
        }
        
        _sizeInGb = sizeG;
        var targetSize = (long)(_sizeInGb * 1024 * 1024 * 1024);
        long currentSize = 0;

        var generatedFileName = $"input_file_{DateTime.Now:yyyyMMddss}_{_sizeInGb}.txt";

        var absPath = Path.Combine(outputFolderPath, generatedFileName);

        if (File.Exists(absPath))
        {
            Console.WriteLine($"File {absPath} already exists. Please delete it or choose a different output folder.");
            return;
        }

        if (!Directory.Exists(outputFolderPath))
        {
            Directory.CreateDirectory(outputFolderPath);
        }

        var buffer = new StringBuilder();

        while (currentSize < targetSize)
        {
            var line = GenerateRandomLine(repetitionRatio);
            buffer.AppendLine(line);
            currentSize += Encoding.UTF8.GetByteCount(line + Environment.NewLine);
            
            if (buffer.Length <= 1024 * 1024 * 100) continue; // Write to file every 100MB
            await AppendTextAndUi(absPath, buffer, currentSize, targetSize);
            buffer.Clear();
        }

        if (buffer.Length > 0)
        {
            await AppendTextAndUi(absPath, buffer, currentSize, targetSize);
        }

        Console.WriteLine("\nFile generation complete.");
    }

    private static async Task AppendTextAndUi(string absPath, StringBuilder buffer, double currentSize,
        double targetSize)
    {
        await File.AppendAllTextAsync(absPath, buffer.ToString());


        // Update progress bar
        var progress = (double)currentSize / targetSize;
        var filledWidth = (int)(progress * BarWidth);
        var progressBar = new string('#', filledWidth).PadRight(BarWidth);
        Console.Write($"\r[{progressBar}] {progress * 100:F2}% done - {currentSize / (1024.0 * 1024 * 1024):F2} GB/{_sizeInGb:F2} GB");
    }

    private static string GenerateRandomLine(double repetitionRatio)
    {
        var randomInt = Random.Next();
        var randomString = GenerateRandomString(10, repetitionRatio);
        return $"{randomInt}. {randomString}";
    }

    private static string GenerateRandomString(int length, double repetitionRatio)
    {
        if (GeneratedStrings.Count > 0 && Random.NextDouble() < repetitionRatio)
        {
            return GeneratedStrings[Random.Next(GeneratedStrings.Count)];
        }

        var stringChars = new char[length];
        for (var i = 0; i < stringChars.Length; i++)
        {
            stringChars[i] = Chars[Random.Next(Chars.Length)];
        }

        var newString = new string(stringChars);
        if (GeneratedStrings.Count < MaxStoredStrings)
        {
            GeneratedStrings.Add(newString);
        }
        else
        {
            // Replace a random old string with the new one to keep the list size constant
            GeneratedStrings[Random.Next(MaxStoredStrings)] = newString;
        }
        return newString;
    }
}
