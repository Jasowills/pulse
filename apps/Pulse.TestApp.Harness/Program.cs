using System.Text.Json;
using Pulse.TestApp.Core;

namespace Pulse.TestApp.Harness;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var provider = TestAppConfig.ParseProvider(GetArg(args, "--provider", "PULSE_PROVIDER", "mongo"));
        var serverDll = GetArg(args, "--server-dll", "PULSE_SERVER_DLL", "");
        if (string.IsNullOrWhiteSpace(serverDll))
        {
            Console.Error.WriteLine("Missing --server-dll (or PULSE_SERVER_DLL).");
            return 2;
        }

        serverDll = Path.GetFullPath(serverDll);
        if (!File.Exists(serverDll))
        {
            Console.Error.WriteLine($"Server DLL not found: {serverDll}");
            return 2;
        }

        var reportPath = GetArg(args, "--report", "PULSE_TESTAPP_REPORT", "");

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            var harness = new Harness(provider, serverDll);
            await harness.RunAsync(cts.Token);

            WriteReport(harness);
            if (!string.IsNullOrWhiteSpace(reportPath))
            {
                await WriteJsonAsync(Path.GetFullPath(reportPath), harness, provider);
            }

            var failed = harness.Results.Count(r => !r.Passed);
            Console.WriteLine();
            foreach (var t in harness.Transitions)
            {
                Console.WriteLine($"  state {t.From} -> {t.To}  @{t.AtSeconds}s");
            }

            Console.WriteLine($"SUMMARY: {harness.Results.Count - failed}/{harness.Results.Count} scenarios passed.");
            return failed == 0 ? 0 : 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"HARNESS FAILED: {ex}");
            return 1;
        }
    }

    private static void WriteReport(Harness harness)
    {
        foreach (var result in harness.Results)
        {
            Console.WriteLine($"{result.Id,-4} {(result.Passed ? "PASS" : "FAIL")}  {result.Name}");
            Console.WriteLine($"      {result.Notes}");
        }
    }

    private static async Task WriteJsonAsync(string path, Harness harness, ProviderKind provider)
    {
        var payload = new
        {
            provider = TestAppConfig.ProviderName(provider),
            timestampUtc = DateTimeOffset.UtcNow,
            scenarios = harness.Results.Select(r => new
            {
                r.Id, r.Name, r.Passed, r.Notes,
            }),
            stateTransitions = harness.Transitions,
        };

        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(path, json);
        Console.WriteLine($"report: {path}");
    }

    private static string GetArg(IReadOnlyList<string> args, string flag, string env, string fallback)
    {
        for (var i = 0; i < args.Count - 1; i++)
        {
            if (args[i] == flag && i + 1 < args.Count)
            {
                return args[i + 1];
            }
        }

        var envValue = Environment.GetEnvironmentVariable(env);
        return string.IsNullOrWhiteSpace(envValue) ? fallback : envValue;
    }
}