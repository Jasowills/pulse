using System.Data;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Pulse.Abstractions;
using Pulse.Abstractions.Json;
using Pulse.Client;
using Pulse.Server;
using Pulse.SqlServer;
using Testcontainers.MsSql;

namespace Pulse.Integration.Tests;

public sealed class SqlServerContainerFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = null!;

    private MsSqlContainer? _container;

    public async Task InitializeAsync()
    {
        var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .Build();
        await container.StartAsync();
        _container = container;

        var databaseName = "pulse_e2e_" + Guid.NewGuid().ToString("N");
        var master = new SqlConnectionStringBuilder(container.GetConnectionString())
        {
            InitialCatalog = "master",
            TrustServerCertificate = true,
        };

        await using (var connection = new SqlConnection(master.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE [{databaseName}]";
            await command.ExecuteNonQueryAsync();
        }

        var builder = new SqlConnectionStringBuilder(container.GetConnectionString())
        {
            InitialCatalog = databaseName,
            TrustServerCertificate = true,
        };
        ConnectionString = builder.ConnectionString;
    }

    public Task DisposeAsync()
        => (_container?.DisposeAsync() ?? ValueTask.CompletedTask).AsTask();
}

/// <summary>
/// End-to-end acceptance for the SQL Server provider: real SQL Server (Testcontainers) +
/// in-process SignalR hub wired via <c>AddSqlServerSource</c> + real client connection.
/// Mirrors the Mongo/Postgres end-to-end suites: filtered snapshot first, then live changes,
/// then restart-resume through the file-backed resume token store.
/// </summary>
public sealed class SqlServerEndToEndTests : IClassFixture<SqlServerContainerFixture>, IAsyncLifetime
{
    private readonly SqlServerContainerFixture _fixture;
    private SqlConnection _conn = null!;
    private string _table = null!;

    public SqlServerEndToEndTests(SqlServerContainerFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _conn = new SqlConnection(_fixture.ConnectionString);
        await _conn.OpenAsync();
        _table = "orders_" + Guid.NewGuid().ToString("N");
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE [{_table}] (
                id nvarchar(50) NOT NULL PRIMARY KEY,
                status nvarchar(50) NOT NULL,
                total bigint NULL
            )
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync()
        => _conn.DisposeAsync().AsTask();

    [Fact]
    public async Task Subscribe_DeliversFilteredSnapshot_ThenLiveChanges()
    {
        await InsertAsync("a", "pending", 50);
        await InsertAsync("b", "shipped", 200);

        var server = await StartServerAsync(_fixture.ConnectionString);
        await using var connection = await ConnectAsync(server.BaseUrl);
        var received = Channel.CreateUnbounded<object>();
        connection.On<PulseChangeMessage>("PulseChange", message => received.Writer.WriteAsync(message).AsTask());
        connection.On<PulseSnapshotMessage>("PulseSnapshot", message => received.Writer.WriteAsync(message).AsTask());

        var subId = await connection.InvokeAsync<string>("Subscribe", _table,
            """{"field":"status","op":"eq","value":"pending"}""");

        var snapshot = Assert.IsType<PulseSnapshotMessage>(await ReadMessageAsync(received));
        Assert.Equal(subId, snapshot.SubscriptionId);
        var doc = Assert.Single(snapshot.Documents);
        Assert.Equal("a", doc["_id"]);

        await InsertAsync("c", "pending", 99);
        var change = Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(received));
        Assert.Equal(subId, change.SubscriptionId);
        Assert.Equal(ChangeKind.Insert, change.Kind);
        Assert.Equal("c", change.DocumentId);
        Assert.Equal("pending", change.Document!["status"]);
        Assert.Equal(99L, change.Document["total"]);

        await InsertAsync("d", "shipped", 5);
        await AssertNoMessageAsync(received);

        await UpdateAsync("c", total: 150);
        var update = Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(received));
        Assert.Equal(ChangeKind.Update, update.Kind);
        Assert.Equal("c", update.DocumentId);
        Assert.Equal(150L, update.Document!["total"]);
        Assert.Equal(150L, update.UpdatedFields!["total"]);

        await server.App.DisposeAsync();
    }

    [Fact]
    public async Task ServerRestart_ResumesFromPersistedToken_WithNoGap()
    {
        var resumeDir = Path.Combine(Path.GetTempPath(), "pulse_sql_resume_it_" + Guid.NewGuid().ToString("N"));
        try
        {
            await InsertAsync("a", "pending", 10);
            await InsertAsync("b", "pending", 20);

            var serverA = await StartServerAsync(_fixture.ConnectionString, resumeDir);
            var connA = await ConnectAsync(serverA.BaseUrl);
            var receivedA = Channel.CreateUnbounded<object>();
            connA.On<PulseChangeMessage>("PulseChange", message => receivedA.Writer.WriteAsync(message).AsTask());
            connA.On<PulseSnapshotMessage>("PulseSnapshot", message => receivedA.Writer.WriteAsync(message).AsTask());
            await connA.InvokeAsync<string>("Subscribe", _table, null);
            Assert.IsType<PulseSnapshotMessage>(await ReadMessageAsync(receivedA));

            await InsertAsync("c", "pending", 30);
            Assert.Equal("c", Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(receivedA)).DocumentId);
            await connA.DisposeAsync();
            await serverA.App.DisposeAsync();

            await WaitUntilAsync(() => Directory.GetFiles(resumeDir).Length == 1);

            await InsertAsync("d", "pending", 40);

            var serverB = await StartServerAsync(_fixture.ConnectionString, resumeDir);
            var connB = await ConnectAsync(serverB.BaseUrl);
            var receivedB = Channel.CreateUnbounded<object>();
            connB.On<PulseChangeMessage>("PulseChange", message => receivedB.Writer.WriteAsync(message).AsTask());
            connB.On<PulseSnapshotMessage>("PulseSnapshot", message => receivedB.Writer.WriteAsync(message).AsTask());
            await connB.InvokeAsync<string>("Subscribe", _table, null);

            var snapshot = Assert.IsType<PulseSnapshotMessage>(await ReadMessageAsync(receivedB));
            Assert.Equal(4, snapshot.Documents.Count);
            Assert.Contains(snapshot.Documents, document => document["_id"] is "d");

            // The resumed watch may replay the change that landed while nothing was watching
            // (a benign duplicate of the snapshot). If it does, it must be 'd' — never a
            // pre-resume document.
            if (await TryReadAsync(receivedB, TimeSpan.FromSeconds(2)) is PulseChangeMessage replay)
            {
                Assert.Equal("d", replay.DocumentId);
            }

            await InsertAsync("e", "pending", 50);
            var live = Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(receivedB));
            Assert.Equal("e", live.DocumentId);

            await connB.DisposeAsync();
            await serverB.App.DisposeAsync();
        }
        finally
        {
            if (Directory.Exists(resumeDir))
            {
                Directory.Delete(resumeDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PulseClient_Subscribes_AndMaintainsCurrent()
    {
        await InsertAsync("a", "pending", 50);
        await InsertAsync("b", "shipped", 200);

        var server = await StartServerAsync(_fixture.ConnectionString);
        await using var client = new PulseClient(server.BaseUrl + "/pulse");
        await client.ConnectAsync();

        var snapshotTcs = new TaskCompletionSource<IReadOnlyList<Order>>();
        var sub = await client.Subscribe<Order>(_table, new FieldCompare("status", CompareOp.Eq, "pending"));
        sub.OnSnapshot += docs => snapshotTcs.TrySetResult(docs);

        var snapshot = await snapshotTcs.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Single(snapshot);
        Assert.Equal("a", snapshot[0]._id);
        Assert.Equal("a", Assert.Single(sub.Current)._id);

        var changes = Channel.CreateUnbounded<PulseChange<Order>>();
        sub.OnChange += change => changes.Writer.TryWrite(change);
        await InsertAsync("c", "pending", 99);
        var insert = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Insert, insert.Kind);
        Assert.Equal("c", insert.DocumentId);
        Assert.Equal(99, insert.Document!.total);
        Assert.Equal(2, sub.Current.Count);

        await UpdateAsync("c", total: 150);
        var update = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Update, update.Kind);
        Assert.Equal(150L, update.UpdatedFields!["total"]);
        Assert.Equal(150, sub.Current.Single(d => d._id == "c").total);

        await DeleteAsync("a");
        var delete = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Delete, delete.Kind);
        Assert.DoesNotContain(sub.Current, d => d._id == "a");

        await sub.UnsubscribeAsync();
        await client.DisposeAsync();
        await server.App.DisposeAsync();
    }

    [Fact]
    public async Task PulseClient_UpdateTransitions_ReflectInCurrent()
    {
        await InsertAsync("a", "pending", 10);
        await InsertAsync("b", "shipped", 20);

        var server = await StartServerAsync(_fixture.ConnectionString);
        await using var client = new PulseClient(server.BaseUrl + "/pulse");
        await client.ConnectAsync();

        var snapshotTcs = new TaskCompletionSource<IReadOnlyList<Order>>();
        var sub = await client.Subscribe<Order>(_table, new FieldCompare("status", CompareOp.Eq, "pending"));
        sub.OnSnapshot += docs => snapshotTcs.TrySetResult(docs);
        await snapshotTcs.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal("a", Assert.Single(sub.Current)._id);

        var changes = Channel.CreateUnbounded<PulseChange<Order>>();
        sub.OnChange += change => changes.Writer.TryWrite(change);

        await UpdateAsync("a", status: "shipped");
        var del = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Delete, del.Kind);
        Assert.Equal("a", del.DocumentId);
        Assert.Null(del.Document);
        Assert.Empty(sub.Current);

        await UpdateAsync("b", status: "pending");
        var ins = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Insert, ins.Kind);
        Assert.Equal("b", ins.DocumentId);
        Assert.Equal("b", Assert.Single(sub.Current)._id);

        await sub.UnsubscribeAsync();
        await client.DisposeAsync();
        await server.App.DisposeAsync();
    }

    private static async Task<(WebApplication App, string BaseUrl)> StartServerAsync(
        string connectionString,
        string? resumeTokenDirectory = null,
        int? port = null)
    {
        var url = port is null ? "http://127.0.0.1:0" : $"http://127.0.0.1:{port}";
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(url);
        builder.Logging.ClearProviders();
        builder.Services.AddSqlServerSource(options =>
        {
            options.ConnectionString = connectionString;
            options.PollInterval = TimeSpan.FromMilliseconds(50);
        });
        if (resumeTokenDirectory is not null)
        {
            builder.Services.AddSingleton<IResumeTokenStore>(new FileResumeTokenStore(resumeTokenDirectory));
        }

        var app = builder.Build();
        app.MapHub<PulseHub>("/pulse");
        await app.StartAsync();
        return (app, app.Urls.First());
    }

    private async Task InsertAsync(string id, string status, long? total = null)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO [{_table}] (id, status, total) VALUES (@id, @status, @total)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@status", status);
        cmd.Parameters.Add("@total", SqlDbType.BigInt).Value = (object?)total ?? DBNull.Value;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task UpdateAsync(string id, string? status = null, long? total = null)
    {
        var sets = new List<string>();
        if (status is not null)
        {
            sets.Add("status = @status");
        }

        if (total is not null)
        {
            sets.Add("total = @total");
        }

        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"UPDATE [{_table}] SET {string.Join(", ", sets)} WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        if (status is not null)
        {
            cmd.Parameters.AddWithValue("@status", status);
        }

        if (total is not null)
        {
            cmd.Parameters.Add("@total", SqlDbType.BigInt).Value = total;
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task DeleteAsync(string id)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM [{_table}] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<HubConnection> ConnectAsync(string baseUrl)
    {
        var connection = new HubConnectionBuilder()
            .WithUrl($"{baseUrl}/pulse")
            .AddJsonProtocol(options =>
                options.PayloadSerializerOptions.Converters.Add(new ObjectToInferredTypesConverter()))
            .Build();
        await connection.StartAsync();
        return connection;
    }

    private static async Task<T> ReadMessageAsync<T>(Channel<T> channel, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(20));
        return await channel.Reader.ReadAsync(cts.Token);
    }

    private static async Task AssertNoMessageAsync<T>(Channel<T> channel)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<OperationCanceledException>(() => channel.Reader.ReadAsync(cts.Token).AsTask());
    }

    private static async Task<T?> TryReadAsync<T>(Channel<T> channel, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await channel.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return default;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("Condition was not satisfied within the timeout.");
            }

            await Task.Delay(50);
        }
    }
}
