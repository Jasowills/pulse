using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.Postgres;

/// <summary>
/// <see cref="IChangeSource"/> backed by Postgres triggers + an append-only change log.
/// Pulse installs a <c>pulse</c> schema with a <c>pulse._changes</c> log and a per-table
/// trigger; the trigger records every insert/update/delete as a JSON row and NOTIFies a
/// channel, which the watcher consumes via LISTEN with a polling fallback. The resume
/// token is the change-log sequence number, so watching can pick up exactly where it left
/// off. Tables must have a single-column primary key (used as the document <c>_id</c>);
/// tables with no or composite primary keys are rejected with an actionable error.
/// Watches for the same source (without a resume token) share one underlying poller and are
/// fanned out internally; a watch resumed from an arbitrary point is private, since a
/// resumed poller cannot be shared.
/// </summary>
public sealed class PostgresChangeSource : IChangeSource
{
    private const string NotificationChannel = "pulse_changes";

    /// <summary>
    /// Idempotent schema bootstrap: the <c>pulse</c> schema, the change log, the source
    /// registry, the <c>changed_fields</c> diff helper, and the generic <c>record_change</c>
    /// trigger function. Per-table triggers are installed by <see cref="EnsureSourceAsync"/>.
    /// </summary>
    private const string BootstrapScript = """
        CREATE SCHEMA IF NOT EXISTS pulse;

        CREATE TABLE IF NOT EXISTS pulse._sources (
            source text PRIMARY KEY,
            pk_column text NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pulse._changes (
            seq bigserial PRIMARY KEY,
            source text NOT NULL,
            kind text NOT NULL,
            document jsonb,
            pk_value jsonb,
            updated_fields jsonb,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );

        CREATE INDEX IF NOT EXISTS pulse_changes_source_seq ON pulse._changes (source, seq);

        CREATE OR REPLACE FUNCTION pulse.changed_fields(new_doc jsonb, old_doc jsonb)
        RETURNS jsonb
        LANGUAGE sql
        IMMUTABLE
        AS $$
            SELECT coalesce(jsonb_object_agg(key, value), '{}'::jsonb)
            FROM (
                SELECT key, value FROM jsonb_each(new_doc)
                EXCEPT
                SELECT key, value FROM jsonb_each(old_doc)
            ) d;
        $$;

        CREATE OR REPLACE FUNCTION pulse.record_change()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            src text;
            pk_col text;
            new_doc jsonb;
            old_doc jsonb;
            seq_val bigint;
        BEGIN
            src := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
            SELECT pk_column INTO pk_col FROM pulse._sources WHERE source = src;
            IF pk_col IS NULL THEN
                RETURN NULL;
            END IF;

            IF TG_OP = 'DELETE' THEN
                old_doc := (to_jsonb(OLD) - pk_col) || jsonb_build_object('_id', to_jsonb(OLD) -> pk_col);
                INSERT INTO pulse._changes (source, kind, document, pk_value)
                VALUES (src, 'delete', NULL, to_jsonb(OLD) -> pk_col)
                RETURNING seq INTO seq_val;
            ELSE
                new_doc := (to_jsonb(NEW) - pk_col) || jsonb_build_object('_id', to_jsonb(NEW) -> pk_col);
                IF TG_OP = 'INSERT' THEN
                    INSERT INTO pulse._changes (source, kind, document, pk_value)
                    VALUES (src, 'insert', new_doc, to_jsonb(NEW) -> pk_col)
                    RETURNING seq INTO seq_val;
                ELSE
                    old_doc := (to_jsonb(OLD) - pk_col) || jsonb_build_object('_id', to_jsonb(OLD) -> pk_col);
                    INSERT INTO pulse._changes (source, kind, document, pk_value, updated_fields)
                    VALUES (src, 'update', new_doc, to_jsonb(NEW) -> pk_col, pulse.changed_fields(new_doc, old_doc))
                    RETURNING seq INTO seq_val;
                END IF;
            END IF;

            PERFORM pg_notify('pulse_changes', src || ':' || seq_val::text);
            RETURN NULL;
        END;
        $$;
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger _logger;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _pruneInterval;
    private readonly object _sync = new();
    private readonly Dictionary<string, string> _pkColumns = new(StringComparer.Ordinal);
    private int _bootstrapped;
    private readonly SharedWatchCoordinator _coordinator;
    private readonly PostgresAdapter _adapter;

    public PostgresChangeSource(
        NpgsqlDataSource dataSource,
        ILogger<PostgresChangeSource>? logger = null,
        TimeSpan? pollInterval = null,
        TimeSpan? pruneInterval = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _logger = logger ?? NullLogger<PostgresChangeSource>.Instance;
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(250);
        _pruneInterval = pruneInterval ?? TimeSpan.FromMinutes(1);
        _adapter = new PostgresAdapter(_dataSource, _pollInterval);
        _coordinator = new SharedWatchCoordinator(
            _adapter,
            new SharedWatchCoordinatorOptions
            {
                PollInterval = _pollInterval,
                MaxBackoff = TimeSpan.FromSeconds(30),
                MaxStaleRetries = 3,
            });
    }

    public string ProviderIdFor(string source)
    {
        var (schema, table) = ResolveSource(source);
        return $"postgres:{schema}.{table}";
    }

    public async Task<IAsyncDisposable> WatchAsync(
        string source,
        Func<ChangeEvent, Task> onChange,
        ResumeToken? resumeFrom,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be a non-empty table name.", nameof(source));
        }

        if (onChange is null)
        {
            throw new ArgumentNullException(nameof(onChange));
        }

        await EnsureSourceAsync(source, cancellationToken).ConfigureAwait(false);

        var resolved = ResolvedSource(source);
        if (resumeFrom is not null)
        {
            ValidateProviderId(source, resumeFrom);
            return await _coordinator.SubscribeResumedAsync(resolved, resumeFrom, onChange, cancellationToken).ConfigureAwait(false);
        }

        return await _coordinator.SubscribeAsync(resolved, onChange, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf)>
        GetSnapshotAsync(string source, SubscriptionFilter filter, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be a non-empty table name.", nameof(source));
        }

        if (filter is null)
        {
            throw new ArgumentNullException(nameof(filter));
        }

        if (filter.Where is not null && !string.Equals(filter.Source, source, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Filter was built for source '{filter.Source}' but the snapshot targets '{source}'.",
                nameof(filter));
        }

        var (schema, table) = ResolveSource(source);
        var resolved = $"{schema}.{table}";
        await EnsureSourceAsync(source, cancellationToken).ConfigureAwait(false);

        string pk;
        lock (_sync)
        {
            pk = _pkColumns[resolved];
        }

        // Watch-first, then snapshot (see README "Resume tokens and gapless delivery"): the
        // as-of sequence is captured before the snapshot query runs, so changes at or after
        // it supersede the snapshot and the caller can deliver without a gap or duplicate.
        var asOfSeq = await GetCurrentMaxSeqAsync(resolved, cancellationToken).ConfigureAwait(false);

        var parameters = new Dictionary<string, object>();
        var where = filter.Where is null ? null : PostgresFilterTranslator.Translate(filter.Where, parameters);

        var sql = new StringBuilder();
        sql.Append("WITH pulse_docs AS (");
        sql.Append($"SELECT (to_jsonb(t) - @pk) || jsonb_build_object('_id', to_jsonb(t) -> @pk) AS doc ");
        sql.Append($"FROM {QuoteIdent(schema)}.{QuoteIdent(table)} t");
        sql.Append(") SELECT doc::text FROM pulse_docs");
        if (where is not null)
        {
            sql.Append(" WHERE ").Append(where);
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql.ToString();
        command.Parameters.AddWithValue("pk", pk);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        var documents = new List<IReadOnlyDictionary<string, object?>>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            using var doc = JsonDocument.Parse(reader.GetString(0));
            documents.Add(JsonValueConverter.ToDictionary(doc.RootElement));
        }

        return (documents, new ResumeToken(ProviderIdFor(source), EncodeSeq(asOfSeq)));
    }

    private async Task EnsureSourceAsync(string source, CancellationToken cancellationToken)
    {
        var (schema, table) = ResolveSource(source);
        var resolved = $"{schema}.{table}";

        if (Interlocked.Exchange(ref _bootstrapped, 1) == 0)
        {
            await BootstrapAsync(cancellationToken).ConfigureAwait(false);
        }

        lock (_sync)
        {
            if (_pkColumns.ContainsKey(resolved))
            {
                return;
            }
        }

        var pk = await GetPrimaryKeyAsync(schema, table, cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var upsert = connection.CreateCommand())
        {
            upsert.CommandText = """
                INSERT INTO pulse._sources (source, pk_column) VALUES (@source, @pk)
                ON CONFLICT (source) DO UPDATE SET pk_column = EXCLUDED.pk_column
                """;
            upsert.Parameters.AddWithValue("source", resolved);
            upsert.Parameters.AddWithValue("pk", pk);
            await upsert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var qualified = $"{QuoteIdent(schema)}.{QuoteIdent(table)}";
        await using (var dropTrigger = connection.CreateCommand())
        {
            dropTrigger.CommandText = $"DROP TRIGGER IF EXISTS pulse_record_change ON {qualified}";
            await dropTrigger.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var createTrigger = connection.CreateCommand())
        {
            createTrigger.CommandText = $"""
                CREATE TRIGGER pulse_record_change
                AFTER INSERT OR UPDATE OR DELETE ON {qualified}
                FOR EACH ROW EXECUTE FUNCTION pulse.record_change()
                """;
            await createTrigger.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        lock (_sync)
        {
            _pkColumns[resolved] = pk;
        }
    }

    private async Task BootstrapAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = BootstrapScript;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the table's single primary-key column, rejecting tables with none or with a
    /// composite key (Pulse identifies documents by a single <c>_id</c>).
    /// </summary>
    private async Task<string> GetPrimaryKeyAsync(string schema, string table, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        await using (var existsCommand = connection.CreateCommand())
        {
            existsCommand.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = @table AND n.nspname = @schema AND c.relkind IN ('r', 'p', 'f')
                )
                """;
            existsCommand.Parameters.AddWithValue("table", table);
            existsCommand.Parameters.AddWithValue("schema", schema);
            if (!(bool)(await existsCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!)
            {
                throw new InvalidOperationException($"Pulse: table '{schema}.{table}' does not exist.");
            }
        }

        var pkColumns = new List<string>();
        await using (var pkCommand = connection.CreateCommand())
        {
            pkCommand.CommandText = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_class t ON t.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary AND n.nspname = @schema AND t.relname = @table
                ORDER BY a.attnum
                """;
            pkCommand.Parameters.AddWithValue("table", table);
            pkCommand.Parameters.AddWithValue("schema", schema);
            using var reader = await pkCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                pkColumns.Add(reader.GetString(0));
            }
        }

        if (pkColumns.Count == 0)
        {
            throw new InvalidOperationException(
                $"Pulse: table '{schema}.{table}' has no primary key. Pulse needs a single-column primary key to identify documents; add one (e.g. 'id bigserial primary key') and try again.");
        }

        if (pkColumns.Count > 1)
        {
            throw new InvalidOperationException(
                $"Pulse: table '{schema}.{table}' has a composite primary key ({string.Join(", ", pkColumns)}). Pulse needs a single-column primary key to identify documents; add one and try again.");
        }

        return pkColumns[0];
    }

    private async Task<long> GetCurrentMaxSeqAsync(string resolved, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COALESCE(MAX(seq), 0) FROM pulse._changes WHERE source = @source";
        command.Parameters.AddWithValue("source", resolved);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    /// <summary>Returns every change after <paramref name="lastSeq"/>, oldest first.</summary>
    private async Task<IReadOnlyList<(long Seq, ChangeEvent Change)>> FetchChangesAsync(
        NpgsqlConnection connection,
        string source,
        string resolved,
        long lastSeq,
        CancellationToken cancellationToken)
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

        var changes = new List<(long Seq, ChangeEvent Change)>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

            changes.Add((seq, new ChangeEvent(
                Source: source,
                Kind: kind,
                DocumentId: pkValue?.ToString() ?? string.Empty,
                FullDocument: document,
                UpdatedFields: updatedFields,
                Token: new ResumeToken(ProviderIdFor(source), EncodeSeq(seq)),
                Timestamp: timestamp)));
        }

        return changes;
    }

    private void ValidateProviderId(string source, ResumeToken resumeFrom) => resumeFrom.EnsureProvider(ProviderIdFor(source));

    private static string ResolvedSource(string source)
    {
        var (schema, table) = ResolveSource(source);
        return $"{schema}.{table}";
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

    private static string QuoteIdent(string name)
        => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static byte[] EncodeSeq(long seq) => BitConverter.GetBytes(seq);

    private static long DecodeSeq(byte[] opaque)
    {
        if (opaque is null || opaque.Length != sizeof(long))
        {
            throw new ResumeTokenInvalidException(
                "Postgres resume tokens must be 8 bytes (a change-log sequence number).");
        }

        return BitConverter.ToInt64(opaque, 0);
    }

}
