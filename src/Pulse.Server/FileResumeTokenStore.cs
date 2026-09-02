using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// File-backed <see cref="IResumeTokenStore"/>: one JSON file per source key, written
/// atomically (temp file + rename). Register this instead of the default in-memory store
/// when a server restart must not drop events.
/// </summary>
public sealed class FileResumeTokenStore : IResumeTokenStore
{
    private readonly string _directory;

    public FileResumeTokenStore(string directory)
    {
        if (string.IsNullOrWhiteSpace(directory))
        {
            throw new ArgumentException("Directory must not be empty.", nameof(directory));
        }

        _directory = directory;
        Directory.CreateDirectory(directory);
    }

    public Task<ResumeToken?> GetAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);

        var path = PathFor(key);
        if (!File.Exists(path))
        {
            // Fallback to pre-hash path for existing installations
            var oldPath = OldPathFor(key);
            if (!File.Exists(oldPath))
            {
                return Task.FromResult<ResumeToken?>(null);
            }

            path = oldPath;
        }

        try
        {
            using var stream = File.OpenRead(path);
            var token = JsonSerializer.Deserialize<ResumeToken>(stream);
            return Task.FromResult(token);
        }
        catch (JsonException ex)
        {
            throw new ResumeTokenInvalidException(
                $"Stored resume token file '{path}' is corrupt and cannot be used: {ex.Message}", ex);
        }
    }

    public async Task SaveAsync(string key, ResumeToken token, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(token);

        var path = PathFor(key);
        var temp = path + ".tmp";
        await using (var stream = File.Create(temp))
        {
            await JsonSerializer.SerializeAsync(stream, token, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        File.Move(temp, path, overwrite: true);
    }

    public Task DeleteAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);

        File.Delete(PathFor(key));
        File.Delete(OldPathFor(key));
        return Task.CompletedTask;
    }

    private string PathFor(string key)
    {
        var safe = new string(key.Select(static c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
        // Append short hash to avoid collisions (e.g. "a:b" vs "a_b" both -> "a_b.json")
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key)))[..8];
        return Path.Combine(_directory, $"{safe}_{hash}.json");
    }

    private string OldPathFor(string key)
    {
        var safe = new string(key.Select(static c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
        return Path.Combine(_directory, safe + ".json");
    }
}
