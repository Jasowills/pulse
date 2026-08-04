using Npgsql;
using NpgsqlTypes;

namespace Pulse.TestApp.Core;

/// <summary>Direct Postgres access. Table columns are PascalCase (quoted) to match the Order model.</summary>
public sealed class PostgresOrderStore : IOrderStore
{
    private const string TableSql = """
        CREATE TABLE IF NOT EXISTS orders (
            "Id" uuid PRIMARY KEY,
            "CustomerName" text NOT NULL,
            "Status" text NOT NULL,
            "Total" numeric(12,2) NOT NULL,
            "Items" integer NOT NULL,
            "CreatedAt" timestamptz NOT NULL,
            "UpdatedAt" timestamptz NOT NULL,
            "Region" text NOT NULL
        )
        """;

    private readonly NpgsqlDataSource _dataSource;

    public PostgresOrderStore(string connectionString)
    {
        _dataSource = NpgsqlDataSource.Create(connectionString);
    }

    public ProviderKind Provider => ProviderKind.Postgres;

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = TableSql;
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE INDEX IF NOT EXISTS orders_status_region ON orders ("Status", "Region");
                CREATE INDEX IF NOT EXISTS orders_updated_at ON orders ("UpdatedAt");
                """;
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public async Task SeedAsync(int count, CancellationToken ct = default)
    {
        var rng = new Random(20260803);
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "TRUNCATE orders";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        var now = DateTimeOffset.UtcNow;
        for (var i = 0; i < count; i++)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO orders ("Id", "CustomerName", "Status", "Total", "Items", "CreatedAt", "UpdatedAt", "Region")
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
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE orders SET "Status" = @status, "UpdatedAt" = @updated WHERE "Id" = @id
            """;
        cmd.Parameters.AddWithValue("status", status);
        cmd.Parameters.AddWithValue("updated", DateTimeOffset.UtcNow);
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task UpdateRegionAsync(string id, string region, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE orders SET "Region" = @region, "UpdatedAt" = @updated WHERE "Id" = @id
            """;
        cmd.Parameters.AddWithValue("region", region);
        cmd.Parameters.AddWithValue("updated", DateTimeOffset.UtcNow);
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<Order?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT "Id", "CustomerName", "Status", "Total", "Items", "CreatedAt", "UpdatedAt", "Region"
            FROM orders WHERE "Id" = @id
            """;
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadOrder(reader) : null;
    }

    public async Task<IReadOnlyList<Order>> GetAllAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT "Id", "CustomerName", "Status", "Total", "Items", "CreatedAt", "UpdatedAt", "Region"
            FROM orders
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
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
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
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT to_regclass('orders') IS NOT NULL";
                var exists = (bool)(await cmd.ExecuteScalarAsync(ct))!;
                requirements.Add(new SetupRequirement("Orders table", exists, exists ? "Present." : "Missing (run seed first)."));
            }

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT to_regclass('pulse._changes') IS NOT NULL";
                var pulseLog = (bool)(await cmd.ExecuteScalarAsync(ct))!;
                requirements.Add(new SetupRequirement(
                    "Pulse change log",
                    pulseLog,
                    pulseLog
                        ? "pulse._changes present (a subscription has run)."
                        : "pulse._changes absent — Pulse creates it on first subscribe."));
            }
        }
        catch (Exception ex)
        {
            requirements.Add(new SetupRequirement("Schema", false, ex.Message));
        }

        return new SetupCheck(requirements);
    }

    private static Order ReadOrder(NpgsqlDataReader reader)
        => new()
        {
            Id = reader.GetGuid(0).ToString(),
            CustomerName = reader.GetString(1),
            Status = reader.GetString(2),
            Total = reader.GetDecimal(3),
            Items = reader.GetInt32(4),
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(5),
            UpdatedAt = reader.GetFieldValue<DateTimeOffset>(6),
            Region = reader.GetString(7),
        };
}