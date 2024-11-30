using BenchmarkDotNet.Running;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

namespace TextFile.Parser;

internal class Program
{
    private static async Task Main(string[] args)
    {
#if BENCHMARK

        var summary = BenchmarkRunner.Run<MicroMergeAsyncBench>();
#else
        try
        {
            var host = CreateHostBuilder(args).Build();
            var parser = host.Services.GetRequiredService<IParser>();
            var startTime = DateTime.Now;
            Log.Information("Processing started at {StartTime:yyyy-MM-dd-hh.mm.ss}", startTime);
            Log.Information("[0.000] Starting to parse and create chunks");
            await parser.CreateExternalChunks();
            var midTime = DateTime.Now;
            Log.Information("[{ElapsedTime:F3}] Starting to merge output file", (midTime - startTime).TotalSeconds);
            await parser.MergeSortedChunks();
            var endTime = DateTime.Now;
            Log.Information("[{ElapsedTime:F3}] Finished processing", (endTime - startTime).TotalSeconds);
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application terminated unexpectedly");
        }
        finally
        {
            await Log.CloseAndFlushAsync();
        }
#endif
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            })
            .UseSerilog((context, services, configuration) => configuration
                .ReadFrom.Configuration(context.Configuration)
                .WriteTo.Console()
                .WriteTo.File("logs/log.txt", rollingInterval: RollingInterval.Day)) // Add this line to configure file logging
            .ConfigureServices((context, services) =>
            {
                services.AddTransient<IParser, ParallelBackgroundWorkers>();
            });
}
