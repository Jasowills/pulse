using Microsoft.Data.SqlClient;

namespace Pulse.TestApp.Core;

/// <summary>Direct SQL Server access. Change tracking is enabled by Pulse on first subscribe.</summary>
public sealed class SqlServerOrderStore : IOrderStore
{
    private const string TableSql = """
        IF OBJECT_ID(N'dbo.orders') IS NULL
        BEGIN
            CREATE TABLE dbo.orders (
                "Id" uniqueidentifier PRIMARY KEY,
                "CustomerName" nvarchar(200) NOT NULL,
                "Status" nvarchar(50) NOT NULL,
                "Total" decimal(12,2) NOT NULL,
                "Items" int NOT NULL,
                "CreatedAt" datetime2 NOT NULL,
                "UpdatedAt" datetime2 NOT NULL,
                "Region" nvarchar(10) NOT NULL
            );
            CREATE INDEX IX_orders_status_region ON dbo.orders ("Status", "Region");
            CREATE INDEX IX_orders_updated_at ON dbo.orders ("UpdatedAt");
        END
        """;

    private readonly string _connectionString;

    public SqlServerOrderStore(string connectionString)
    {
        _connectionString = connectionString;
    }

    public ProviderKind Provider => ProviderKind.SqlServer;

    private SqlConnection Open() => new(_connectionString);

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var connection = Open();
        await connection.OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = TableSql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task SeedAsync(int count, CancellationToken ct = default)
    {
        var rng = new Random(20260803);
        await using var connection = Open();
        await connection.OpenAsync(ct);
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM dbo.orders";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        var now = DateTime.UtcNow;
        for (var i = 0; i < count; i++)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO dbo.orders ("Id", "CustomerName", "Status", "Total", "Items", "CreatedAt", "UpdatedAt", "Region")
                VALUES (@id, @name, @status, @total, @items, @created, @updated, @region)
                """;
            cmd.Parameters.AddWithValue("id", Guid.NewGuid());
            cmd.Parameters.AddWithValue("name", $"Customer {rng.Next(1, 500)}");
            cmd.Parameters.AddWithValue("status", OrderState.Statuses[rng.Next(OrderState.Statuses.Length)]);
            cmd.Parameters.AddWithValue("total", rng.Next(1, 2000) + (rng.Next(1, 100) / 100m));
            cmd.Parameters.AddWithValue("items", rng.Next(1, 20));
            cmd.Parameters.AddWithValue("created", now.AddMinutes(-i));
            cmd.Parameters.AddWithValue("updated", now);
            cmd.Parameters.AddWithValue("region", OrderState.Regions[rng.Next(OrderState.Regions.Length)]);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public async Task UpdateStatusAsync(string id, string status, CancellationToken ct = default)
    {
        await using var connection = Open();
        await connection.OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE dbo.orders SET "Status" = @status, "UpdatedAt" = @updated WHERE "Id" = @id
            """;
        cmd.Parameters.AddWithValue("status", status);
        cmd.Parameters.AddWithValue("updated", DateTime.UtcNow);
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task UpdateRegionAsync(string id, string region, CancellationToken ct = default)
    {
        await using var connection = Open();
        await connection.OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE dbo.orders SET "Region" = @region, "UpdatedAt" = @updated WHERE "Id" = @id
            """;
        cmd.Parameters.AddWithValue("region", region);
        cmd.Parameters.AddWithValue("updated", DateTime.UtcNow);
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<Order?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        await using var connection = Open();
        await connection.OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT "Id", "CustomerName", "Status", "Total", "Items", "CreatedAt", "UpdatedAt", "Region"
            FROM dbo.orders WHERE "Id" = @id
            """;
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadOrder(reader) : null;
    }

    public async Task<IReadOnlyList<Order>> GetAllAsync(CancellationToken ct = default)
    {
        await using var connection = Open();
        await connection.OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT "Id", "CustomerName", "Status", "Total", "Items", "CreatedAt", "UpdatedAt", "Region"
            FROM dbo.orders
            """;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var list = new List<Order>();
        while (await reader.ReadAsync(ct))
        {
            list.Add(ReadOrder(reader));
        }

        return list;
    }

    public async Task<int> BulkMutateAsync(int count, int intervalMs, Random rng, CancellationToken ct = default)
    {
        var all = await GetAllAsync(ct);
        if (all.Count == 0)
        {
            return 0;
        }

        var applied = 0;
        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();
            var target = all[rng.Next(all.Count)];
            var newStatus = OrderState.Statuses[rng.Next(OrderState.Statuses.Length)];
            await UpdateStatusAsync(target.Id, newStatus, ct);
            applied++;
            if (intervalMs > 0)
            {
                await Task.Delay(intervalMs, ct);
            }
        }

        return applied;
    }

    public async Task<SetupCheck> VerifySetupAsync(CancellationToken ct = default)
    {
        var requirements = new List<SetupRequirement>();
        try
        {
            await using var connection = Open();
            await connection.OpenAsync(ct);
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteScalarAsync(ct);
            }

            requirements.Add(new SetupRequirement("Connection", true, "Connected."));
        }
        catch (Exception ex)
        {
            requirements.Add(new SetupRequirement("Connection", false, ex.Message));
        }

        try
        {
            await using var connection = Open();
            await connection.OpenAsync(ct);
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT DATABASEPROPERTYEX(DB_NAME(), 'IsChangeTrackingEnabled')";
                var enabled = await cmd.ExecuteScalarAsync(ct);
                var on = enabled is int i && i == 1;
                requirements.Add(new SetupRequirement(
                    "DB change tracking",
                    on,
                    on
                        ? "Enabled (or Pulse will enable it on first subscribe)."
                        : "Disabled — Pulse enables it on first subscribe (needs ALTER DATABASE permission)."));
            }

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(N'dbo.orders')";
                var tableTracked = await cmd.ExecuteScalarAsync(ct);
                requirements.Add(new SetupRequirement(
                    "Orders change tracking",
                    tableTracked is not null,
                    tableTracked is not null
                        ? "Tracked (a subscription has run)."
                        : "Not yet tracked — Pulse enables it on first subscribe."));
            }
        }
        catch (Exception ex)
        {
            requirements.Add(new SetupRequirement("Change tracking", false, ex.Message));
        }

        return new SetupCheck(requirements);
    }

    private static Order ReadOrder(SqlDataReader reader)
        => new()
        {
            Id = reader.GetGuid(0).ToString(),
            CustomerName = reader.GetString(1),
            Status = reader.GetString(2),
            Total = reader.GetDecimal(3),
            Items = reader.GetInt32(4),
            CreatedAt = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)),
            UpdatedAt = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc)),
            Region = reader.GetString(7),
        };
}