namespace Pulse.TestApp.Core;

/// <summary>Result of <see cref="IOrderStore.VerifySetupAsync"/> for one prerequisite.</summary>
public sealed record SetupRequirement(string Name, bool Passed, string Detail);

/// <summary>Aggregate of a provider's setup verification.</summary>
public sealed class SetupCheck
{
    public SetupCheck(IReadOnlyList<SetupRequirement> requirements)
    {
        Requirements = requirements;
    }

    public IReadOnlyList<SetupRequirement> Requirements { get; }

    public bool AllPassed => Requirements.All(r => r.Passed);
}

/// <summary>
/// Direct database access to the <c>orders</c> entity for the seed tool and the test
/// server/harness's "external writer" writes. These writes deliberately go straight to the
/// database through the provider's driver — never through Pulse — so they model real
/// applications mutating state independently of the change feed.
/// </summary>
public interface IOrderStore
{
    ProviderKind Provider { get; }

    /// <summary>Ensures the orders table/collection and needed indexes exist.</summary>
    Task InitializeAsync(CancellationToken ct = default);

    /// <summary>Truncates the orders (collection/table) and seeds <paramref name="count"/> rows.</summary>
    Task SeedAsync(int count, CancellationToken ct = default);

    /// <summary>Updates one order's status to <paramref name="status"/> and bumps UpdatedAt.</summary>
    Task UpdateStatusAsync(string id, string status, CancellationToken ct = default);

    /// <summary>Updates one order's region to <paramref name="region"/> and bumps UpdatedAt (used to force filter transition-in).</summary>
    Task UpdateRegionAsync(string id, string region, CancellationToken ct = default);

    /// <summary>Reads the single order by id (string form of the provider key).</summary>
    Task<Order?> GetByIdAsync(string id, CancellationToken ct = default);

    /// <summary>Reads every order directly from the database (for expected-state verification).</summary>
    Task<IReadOnlyList<Order>> GetAllAsync(CancellationToken ct = default);

    /// <summary>Rapidly flips <paramref name="count"/> random orders' status, sleeping <paramref name="intervalMs"/> between each. Returns the number applied.</summary>
    Task<int> BulkMutateAsync(int count, int intervalMs, Random rng, CancellationToken ct = default);

    /// <summary>Checks the provider prerequisites Pulse needs (replica set, change tracking, permissions).</summary>
    Task<SetupCheck> VerifySetupAsync(CancellationToken ct = default);
}

public static class OrderStoreFactory
{
    public static IOrderStore Create(ProviderKind provider)
        => provider switch
        {
            ProviderKind.Mongo => new MongoOrderStore(TestAppConfig.MongoUri, TestAppConfig.MongoDatabase),
            ProviderKind.Postgres => new PostgresOrderStore(TestAppConfig.PostgresConnectionString),
            ProviderKind.SqlServer => new SqlServerOrderStore(TestAppConfig.SqlServerConnectionString),
            _ => throw new ArgumentOutOfRangeException(nameof(provider)),
        };
}