using System.Globalization;
using System.Text.Json;
using Npgsql;
using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.Postgres;

/// <summary>
/// Adapter behind <see cref="SharedWatchCoordinator"/> for Postgres.
/// Owns poll primitives (MAX(seq) position, batched fetch, LISTEN wait) and
/// translates rows to <see cref="ChangeEvent"/>.
/// </summary>
internal sealed class PostgresAdapter : IChangePollAdapter
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly TimeSpan _pollInterval;

    public PostgresAdapter(NpgsqlDataSource dataSource, TimeSpan pollInterval)
    {
        _dataSource = dataSource;
        _pollInterval = pollInterval;
    }

    public string ProviderIdFor(string resolvedSource)
    {
        var (schema, table) = ResolveSource(resolvedSource);
        return $"postgres:{schema}.{table}";
    }

    public async Task<ResumeToken> GetCurrentPositionAsync(string resolvedSource, CancellationToken cancellationToken)
    {
        var seq = await GetCurrentMaxSeqAsync(resolvedSource, cancellationToken).ConfigureAwait(false);
        return new ResumeToken(ProviderIdFor(resolvedSource), EncodeSeq(seq));
    }

    public async Task<PollBatch> PollAsync(string resolvedSource, ResumeToken after, CancellationToken cancellationToken)
    {
        var afterSeq = DecodeSeq(after.Opaque);
        // PrivateWatch stale check: token must not point past current head.
        // SharedWatch also benefits — if its last position was pruned and seq is beyond head, treat as stale elsewhere;
        // here we only guard the obvious case for retained tokens.
        var maxSeq = await GetCurrentMaxSeqAsync(resolvedSource, cancellationToken).ConfigureAwait(false);
        if (afterSeq > maxSeq)
        {
            throw new ResumeTokenInvalidException(
                $"Resume token for '{ProviderIdFor(resolvedSource)}' points past the current change log (seq {afterSeq} > current {maxSeq}). The token is stale or was issued against a different database; refusing to resume from it.");
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var changes = await FetchChangesAsync(connection, resolvedSource, resolvedSource, afterSeq, cancellationToken).ConfigureAwait(false);
        if (changes.Count == 0)
        {
            return new PollBatch(Array.Empty<ChangeEvent>(), after);
        }

        var events = new List<ChangeEvent>(changes.Count);
        ResumeToken lastToken = after;
        foreach (var (seq, change) in changes)
        {
            events.Add(change);
            lastToken = change.Token;
        }

        return new PollBatch(events, lastToken);
    }

    public async Task WaitAsync(string resolvedSource, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using (var listen = connection.CreateCommand())
            {
                listen.CommandText = "LISTEN pulse_changes";
                await listen.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(_pollInterval);
            try
            {
                await connection.WaitAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (NpgsqlException) { }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // Best-effort wait; poll will retry via loop delay on next iteration.
            try
            {
                await Task.Delay(_pollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }

    // Copy of PostgresChangeSource helpers — kept local so adapter is self-contained.
    private async Task<long> GetCurrentMaxSeqAsync(string resolvedSource, CancellationToken ct)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COALESCE(MAX(seq), 0) FROM pulse._changes WHERE source = @source";
        command.Parameters.AddWithValue("source", resolvedSource);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private static async Task<IReadOnlyList<(long Seq, ChangeEvent Change)>> FetchChangesAsync(
        NpgsqlConnection connection,
        string source,
        string resolved,
        long lastSeq,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT seq, kind, document::text, pk_value::text, updated_fields::text, created_at
            FROM pulse._changes
            WHERE source = @source AND seq > @seq
            ORDER BY seq
            """;
        command.Parameters.AddWithValue("source", resolved);
        command.Parameters.AddWithValue("seq", lastSeq);

        var changes = new List<(long, ChangeEvent)>();
        using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var seq = reader.GetInt64(0);
            var kindText = reader.GetString(1);
            var documentJson = reader.IsDBNull(2) ? null : reader.GetString(2);
            var pkJson = reader.IsDBNull(3) ? null : reader.GetString(3);
            var updatedJson = reader.IsDBNull(4) ? null : reader.GetString(4);
            var timestamp = reader.GetFieldValue<DateTimeOffset>(5);

            var kind = kindText switch
            {
                "insert" => ChangeKind.Insert,
                "update" => ChangeKind.Update,
                "delete" => ChangeKind.Delete,
                _ => throw new InvalidOperationException($"Unknown change kind '{kindText}' in pulse._changes."),
            };

            var pkValue = pkJson is null ? null : JsonValueConverter.ToClrValue(JsonDocument.Parse(pkJson).RootElement);
            var document = documentJson is null ? null : JsonValueConverter.ToDictionary(JsonDocument.Parse(documentJson).RootElement);
            var updatedFields = kind == ChangeKind.Update && updatedJson is not null
                ? JsonValueConverter.ToDictionary(JsonDocument.Parse(updatedJson).RootElement)
                : null;

            // ProviderId must match what ProviderIdFor returns for this resolved source.
            var providerId = $"postgres:{resolved}";
            changes.Add((seq, new ChangeEvent(
                Source: source,
                Kind: kind,
                DocumentId: pkValue?.ToString() ?? string.Empty,
                FullDocument: document,
                UpdatedFields: updatedFields,
                Token: new ResumeToken(providerId, EncodeSeq(seq)),
                Timestamp: timestamp)));
        }

        return changes;
    }

    public void OnFloorAdvanced(string resolvedSource, ResumeToken floor)
    {
        // Fire-and-forget prune: delete rows below floor. Sources with no active watcher never call this, so persisted tokens survive.
        var floorSeq = DecodeSeq(floor.Opaque);
        _ = Task.Run(async () =>
        {
            try
            {
                await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
                await using var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM pulse._changes WHERE source = @source AND seq < @floor";
                command.Parameters.AddWithValue("source", resolvedSource);
                command.Parameters.AddWithValue("floor", floorSeq);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch { }
        });
    }

    private static (string Schema, string Table) ResolveSource(string source)
    {
        var dot = source.IndexOf('.');
        if (dot <= 0 || dot == source.Length - 1)
        {
            return ("public", source);
        }

        return (source[..dot], source[(dot + 1)..]);
    }

    private static byte[] EncodeSeq(long seq) => Int64ResumeTokenCodec.Encode(seq);
    private static long DecodeSeq(byte[] opaque) => Int64ResumeTokenCodec.Decode(opaque);
}
