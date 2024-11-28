using BenchmarkDotNet.Running;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TextFile.Parser;

internal class Program
{
    private static Task Main(string[] args)
    {
        var host = CreateHostBuilder(args).Build();
        var parser = host.Services.GetRequiredService<IParser>();
        ParsingRunner.SetParser(parser);
        //await ParsingRunner.Start();
        BenchmarkRunner.Run<ParsingRunner>();
        Console.WriteLine("File parsing and sorting complete.");
        return Task.CompletedTask;
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            })
            .ConfigureServices((context, services) =>
            {
                services.AddTransient<IParser, ParallelBackgroundWorkers>(); 
            });
}