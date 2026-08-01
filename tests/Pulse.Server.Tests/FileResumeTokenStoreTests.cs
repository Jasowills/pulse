using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.Server.Tests;

public sealed class FileResumeTokenStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "pulse_resume_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Fact]
    public async Task RoundTripsAcrossInstances_AndDeletes()
    {
        var store = new FileResumeTokenStore(_directory);
        var token = new ResumeToken("mongo:db.orders", new byte[] { 1, 2, 3 });

        await store.SaveAsync("mongo:db.orders", token, CancellationToken.None);
        var loaded = await store.GetAsync("mongo:db.orders", CancellationToken.None);
        Assert.Equal(token, loaded);

        // A new instance sees the persisted file.
        var fresh = new FileResumeTokenStore(_directory);
        var reloaded = await fresh.GetAsync("mongo:db.orders", CancellationToken.None);
        Assert.Equal(token, reloaded);

        await fresh.DeleteAsync("mongo:db.orders", CancellationToken.None);
        Assert.Null(await fresh.GetAsync("mongo:db.orders", CancellationToken.None));
    }

    [Fact]
    public async Task UnknownKey_ReturnsNull()
    {
        var store = new FileResumeTokenStore(_directory);
        Assert.Null(await store.GetAsync("mongo:db.missing", CancellationToken.None));
    }

    [Fact]
    public async Task Overwrite_ReplacesPreviousToken()
    {
        var store = new FileResumeTokenStore(_directory);
        await store.SaveAsync("orders", new ResumeToken("fake:orders", new byte[] { 1 }), CancellationToken.None);
        await store.SaveAsync("orders", new ResumeToken("fake:orders", new byte[] { 2 }), CancellationToken.None);

        var loaded = await store.GetAsync("orders", CancellationToken.None);
        Assert.Equal(new byte[] { 2 }, loaded!.Opaque);
    }

    [Fact]
    public async Task CorruptFile_ThrowsResumeTokenInvalid()
    {
        var store = new FileResumeTokenStore(_directory);
        await File.WriteAllTextAsync(Path.Combine(_directory, "orders.json"), "not json");

        await Assert.ThrowsAsync<ResumeTokenInvalidException>(
            () => store.GetAsync("orders", CancellationToken.None));
    }
}
