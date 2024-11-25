using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

class Program
{
    private static readonly Random Random = new Random();
    private static readonly char[] Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ".ToCharArray();
    private static readonly List<string> GeneratedStrings = [];
    private const int MaxStoredStrings = 10000; // Limit the number of stored strings

    static void Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("Usage: <size_in_gb> <output_folder_path> <repetition_ratio>");
            return;
        }

        if (!double.TryParse(args[0], out var sizeInGb))
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

        var targetSize = (long)(sizeInGb * 1024 * 1024 * 1024);
        long currentSize = 0;
        const int barWidth = 50;

        var generatedFileName = $"input_file_{DateTime.Now:yyyyMMdd}_{sizeInGb}.txt";

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

        using (var writer = new StreamWriter(absPath, false, Encoding.UTF8, 65536))
        {
            while (currentSize < targetSize)
            {
                var line = GenerateRandomLine(repetitionRatio);
                writer.WriteLine(line);
                currentSize += Encoding.UTF8.GetByteCount(line + Environment.NewLine);

                // Update progress bar
                var progress = (double)currentSize / targetSize;
                var filledWidth = (int)(progress * barWidth);
                var progressBar = new string('#', filledWidth).PadRight(barWidth);

                Console.Write($"\r[{progressBar}] {progress * 100:F2}% done - {currentSize / (1024.0 * 1024 * 1024):F2} GB/{sizeInGb:F2} GB");
            }
        }

        Console.WriteLine("\nFile generation complete.");
    }

    static string GenerateRandomLine(double repetitionRatio)
    {
        var randomInt = Random.Next();
        var randomString = GenerateRandomString(10, repetitionRatio);
        return $"{randomInt}. {randomString}";
    }

    static string GenerateRandomString(int length, double repetitionRatio)
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