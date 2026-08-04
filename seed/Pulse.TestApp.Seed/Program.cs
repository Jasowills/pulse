using Pulse.TestApp.Core;

namespace Pulse.TestApp.Seed;

/// <summary>
/// Seed + bulk-mutate + verify-setup tooling for all three providers. Every mutation goes
/// straight to the database through the provider driver, never through Pulse — this is how
/// the test harness simulates real external writers.
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var argv = new List<string>(args);
            var providerArg = TakeValue(argv, "--provider") ?? Environment.GetEnvironmentVariable("PULSE_PROVIDER");
            if (string.IsNullOrEmpty(providerArg))
            {
                Console.Error.WriteLine("Missing --provider (mongo|postgres|sqlserver) or PULSE_PROVIDER env.");
                return 2;
            }

            var kind = TestAppConfig.ParseProvider(providerArg);
            var store = OrderStoreFactory.Create(kind);
            if (argv.Count == 0)
            {
                PrintUsage();
                return 2;
            }

            var command = argv[0].ToLowerInvariant();
            return command switch
            {
                "seed" => await SeedAsync(store, argv),
                "bulk-mutate" => await BulkMutateAsync(store, argv),
                "update-status" => await UpdateStatusAsync(store, argv),
                "verify-setup" => await VerifySetupAsync(store),
                _ => UsageError($"Unknown command '{command}'."),
            };
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ERROR: {ex.Message}");
            return 1;
        }
    }

    private static async Task<int> SeedAsync(IOrderStore store, List<string> argv)
    {
        var count = int.Parse(TakeValue(argv, "--count") ?? "50");
        Console.WriteLine($"[{TestAppConfig.ProviderName(store.Provider)}] ensuring schema and seeding {count} orders...");
        await store.InitializeAsync();
        await store.SeedAsync(count);
        var all = await store.GetAllAsync();
        Console.WriteLine($"[{TestAppConfig.ProviderName(store.Provider)}] seeded {all.Count} orders.");
        foreach (var status in OrderState.Statuses)
        {
            var perStatus = all.Count(o => o.Status == status);
            Console.WriteLine($"    status {status,-12} {perStatus}");
        }

        return 0;
    }

    private static async Task<int> BulkMutateAsync(IOrderStore store, List<string> argv)
    {
        var count = int.Parse(TakeValue(argv, "--count") ?? "100");
        var intervalMs = int.Parse(TakeValue(argv, "--interval-ms") ?? "10");
        await store.InitializeAsync();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var applied = await store.BulkMutateAsync(count, intervalMs, new Random(0));
        sw.Stop();
        Console.WriteLine(
            $"[{TestAppConfig.ProviderName(store.Provider)}] bulk-mutated {applied} orders in {sw.ElapsedMilliseconds} ms.");
        return 0;
    }

    private static async Task<int> UpdateStatusAsync(IOrderStore store, List<string> argv)
    {
        if (argv.Count < 3)
        {
            Console.Error.WriteLine("update-status requires <id> <status>.");
            return 2;
        }

        var id = argv[1];
        var status = argv[2];
        if (!OrderState.Statuses.Contains(status))
        {
            Console.Error.WriteLine($"Unknown status '{status}'.");
            return 2;
        }

        await store.InitializeAsync();
        await store.UpdateStatusAsync(id, status);
        Console.WriteLine($"[{TestAppConfig.ProviderName(store.Provider)}] updated order {id} -> {status}.");
        return 0;
    }

    private static async Task<int> VerifySetupAsync(IOrderStore store)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var check = await store.VerifySetupAsync();
        sw.Stop();
        Console.WriteLine($"[{TestAppConfig.ProviderName(store.Provider)}] setup verification ({sw.Elapsed.TotalMilliseconds:0} ms):");
        foreach (var requirement in check.Requirements)
        {
            Console.WriteLine($"    [{(requirement.Passed ? "PASS" : "FAIL")}] {requirement.Name}: {requirement.Detail}");
        }

        return check.AllPassed ? 0 : 1;
    }

    private static int UsageError(string message)
    {
        Console.Error.WriteLine(message);
        PrintUsage();
        return 2;
    }

    private static int UsageWithProvider(string command)
    {
        PrintUsage();
        return 2;
    }

    private static void PrintUsage()
    {
        Console.WriteLine("""
            Usage:
              Pulse.TestApp.Seed --provider <mongo|postgres|sqlserver> verify-setup
              Pulse.TestApp.Seed --provider <p> seed [--count N]
              Pulse.TestApp.Seed --provider <p> bulk-mutate [--count N] [--interval-ms M]
              Pulse.TestApp.Seed --provider <p> update-status <id> <status>
            Env: PULSE_PROVIDER, PULSE_MONGO_URI, PULSE_MONGO_DB, PULSE_POSTGRES, PULSE_SQLSERVER
            """);
    }

    private static string? TakeValue(List<string> argv, string key)
    {
        for (var i = 0; i < argv.Count; i++)
        {
            if (!string.Equals(argv[i], key, StringComparison.Ordinal) || i + 1 >= argv.Count)
            {
                continue;
            }

            var value = argv[i + 1];
            argv.RemoveRange(i, 2);
            return value;
        }

        return null;
    }
}