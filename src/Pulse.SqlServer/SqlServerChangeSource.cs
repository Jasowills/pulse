using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.SqlServer;

/// <summary>
/// <see cref="IChangeSource"/> backed by SQL Server Change Tracking. Pulse enables change
/// tracking on the database and per table (with column tracking), then polls
/// <c>CHANGETABLE(CHANGES ...)</c> for rows changed after the last synchronized version.
/// The resume token is the change-tracking version, so watching can pick up exactly where it
/// left off. Tables must have a single-column primary key (used as the document <c>_id</c>);
/// tables with no or composite primary keys are rejected with an actionable error.
/// Watches for the same source (without a resume token) share one underlying poller and are
/// fanned out internally; a watch resumed from an arbitrary point is private, since a
/// resumed poller cannot be shared.
/// </summary>
public sealed class SqlServerChangeSource : IChangeSource, IChangePollAdapter
{
    private readonly string _connectionString;
    private readonly string _databaseName;
    private readonly ILogger _logger;
    private readonly TimeSpan _pollInterval;
    private readonly object _sync = new();
    private readonly Dictionary<string, string> _pkColumns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<ColumnInfo>> _columns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _maskCache = new(StringComparer.Ordinal);
    private int _dbBootstrapped;

    private readonly SharedWatchCoordinator _coordinator;

    public SqlServerChangeSource(
        string connectionString,
        ILogger<SqlServerChangeSource>? logger = null,
        TimeSpan? pollInterval = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("A connection string is required.", nameof(connectionString));
        }

        _connectionString = connectionString;
        _databaseName = ParseDatabaseName(connectionString);
        _logger = logger ?? NullLogger<SqlServerChangeSource>.Instance;
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(250);
        _coordinator = new SharedWatchCoordinator(this, new SharedWatchCoordinatorOptions { PollInterval = _pollInterval });
    }

    public string ProviderIdFor(string source)
    {
        var (schema, table) = ResolveSource(source);
        return $"sqlserver:{schema}.{table}";
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

        var resolved = ResolvedSourceFor(source);
        if (resumeFrom is not null)
        {
            ValidateProviderId(source, resumeFrom);
            return await _coordinator.SubscribeResumedAsync(resolved, resumeFrom, onChange, cancellationToken).ConfigureAwait(false);
        }

        return await _coordinator.SubscribeAsync(resolved, onChange, cancellationToken).ConfigureAwait(false);
    }

    private static string ResolvedSourceFor(string source)
    {
        var (schema, table) = ResolveSource(source);
        return $"{schema}.{table}";
    }

    // IChangePollAdapter
    public async Task<ResumeToken> GetCurrentPositionAsync(string resolvedSource, CancellationToken cancellationToken)
    {
        var version = await GetCurrentVersionAsync(cancellationToken).ConfigureAwait(false);
        return new ResumeToken(ProviderIdFor(resolvedSource), EncodeVersion(version));
    }

    public async Task<PollBatch> PollAsync(string resolvedSource, ResumeToken after, CancellationToken cancellationToken)
    {
        var afterVersion = DecodeVersion(after.Opaque);
        var current = await GetCurrentVersionAsync(cancellationToken).ConfigureAwait(false);
        if (afterVersion > current)
        {
            throw new ResumeTokenInvalidException(
                $"Resume token for '{ProviderIdFor(resolvedSource)}' points past the current version (version {afterVersion} > current {current}).");
        }

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var changes = await FetchChangesAsync(connection, resolvedSource, resolvedSource, afterVersion, cancellationToken).ConfigureAwait(false);
        if (changes.Count == 0)
        {
            return new PollBatch(Array.Empty<ChangeEvent>(), after);
        }

        var events = changes.Select(c => c.Change).ToList();
        return new PollBatch(events, events[^1].Token);
    }

    public Task WaitAsync(string resolvedSource, CancellationToken cancellationToken)
        => Task.Delay(_pollInterval, cancellationToken);

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
        var qualified = $"{QuoteIdent(schema)}.{QuoteIdent(table)}";
        await EnsureSourceAsync(source, cancellationToken).ConfigureAwait(false);

        string pk;
        IReadOnlyList<ColumnInfo> columns;
        lock (_sync)
        {
            pk = _pkColumns[resolved];
            columns = _columns[resolved];
        }

        // Watch-first, then snapshot (see README "Resume tokens and gapless delivery"): the
        // as-of version is captured before the snapshot query runs, so changes at or after it
        // supersede the snapshot and the caller can deliver without a gap or duplicate.
        var asOfVersion = await GetCurrentVersionAsync(cancellationToken).ConfigureAwait(false);

        var parameters = new Dictionary<string, object>();
        var where = filter.Where is null
            ? null
            : SqlServerFilterTranslator.Translate(filter.Where, pk, parameters);

        var sql = new StringBuilder();
        sql.Append("SELECT ");
        sql.Append($"{QuoteIdent(pk)} AS [_id], ");
        sql.Append(string.Join(", ", columns.Select(c => QuoteIdent(c.Name))));
        sql.Append(" FROM ").Append(qualified);
        if (where is not null)
        {
            sql.Append(" WHERE ").Append(where);
        }

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql.ToString();
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        var documents = new List<IReadOnlyDictionary<string, object?>>();
        using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                documents.Add(ReadDocument(reader, 0, pk, columns));
            }
        }

        return (documents, new ResumeToken(ProviderIdFor(source), EncodeVersion(asOfVersion)));
    }

    private async Task EnsureSourceAsync(string source, CancellationToken cancellationToken)
    {
        var (schema, table) = ResolveSource(source);
        var resolved = $"{schema}.{table}";
        var qualified = $"{QuoteIdent(schema)}.{QuoteIdent(table)}";

        await BootstrapDatabaseAsync(cancellationToken).ConfigureAwait(false);

        lock (_sync)
        {
            if (_pkColumns.ContainsKey(resolved))
            {
                return;
            }
        }

        var pk = await GetPrimaryKeyAsync(resolved, cancellationToken).ConfigureAwait(false);
        var columns = await GetColumnsAsync(resolved, pk, cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var info = connection.CreateCommand())
        {
            info.CommandText = """
                SELECT is_track_columns_updated_on
                FROM sys.change_tracking_tables
                WHERE object_id = OBJECT_ID(@t)
                """;
            info.Parameters.AddWithValue("@t", resolved);
            var raw = await info.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (raw is null or DBNull)
            {
                await using var enable = connection.CreateCommand();
                enable.CommandText = $"ALTER TABLE {qualified} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)";
                await enable.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (Convert.ToInt32(raw, CultureInfo.InvariantCulture) == 0)
            {
                // Tracked but without column info: re-enable so updated_fields can be decoded.
                await using var disable = connection.CreateCommand();
                disable.CommandText = $"ALTER TABLE {qualified} DISABLE CHANGE_TRACKING";
                await disable.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                await using var enable = connection.CreateCommand();
                enable.CommandText = $"ALTER TABLE {qualified} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)";
                await enable.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        lock (_sync)
        {
            _pkColumns[resolved] = pk;
            _columns[resolved] = columns;
        }
    }

    private async Task BootstrapDatabaseAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _dbBootstrapped, 1) != 0)
        {
            return;
        }

        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var check = connection.CreateCommand();
            check.CommandText = "SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()";
            var enabled = Convert.ToInt32(await check.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
            if (enabled == 0)
            {
                await using var command = connection.CreateCommand();
                command.CommandText = $"ALTER DATABASE {QuoteIdent(_databaseName)} SET CHANGE_TRACKING = ON";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            // Reset so a transient/permission failure retries on the next call instead of
            // permanently short-circuiting bootstrap.
            Interlocked.Exchange(ref _dbBootstrapped, 0);
            throw;
        }
    }

    /// <summary>
    /// Returns the table's single primary-key column, rejecting tables with none or with a
    /// composite key (Pulse identifies documents by a single <c>_id</c>).
    /// </summary>
    private async Task<string> GetPrimaryKeyAsync(string resolved, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        await using (var existsCommand = connection.CreateCommand())
        {
            existsCommand.CommandText = "SELECT OBJECT_ID(@t)";
            existsCommand.Parameters.AddWithValue("@t", resolved);
            if (await existsCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is null or DBNull)
            {
                throw new InvalidOperationException($"Pulse: table '{resolved}' does not exist.");
            }
        }

        var pkColumns = new List<string>();
        await using (var pkCommand = connection.CreateCommand())
        {
            pkCommand.CommandText = """
                SELECT c.name
                FROM sys.indexes i
                JOIN sys.index_columns ic
                    ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                JOIN sys.columns c
                    ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(@t)
                ORDER BY ic.key_ordinal
                """;
            pkCommand.Parameters.AddWithValue("@t", resolved);
            using var reader = await pkCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                pkColumns.Add(reader.GetString(0));
            }
        }

        if (pkColumns.Count == 0)
        {
            throw new InvalidOperationException(
                $"Pulse: table '{resolved}' has no primary key. Pulse needs a single-column primary key to identify documents; add one (e.g. 'id bigint identity primary key') and try again.");
        }

        if (pkColumns.Count > 1)
        {
            throw new InvalidOperationException(
                $"Pulse: table '{resolved}' has a composite primary key ({string.Join(", ", pkColumns)}). Pulse needs a single-column primary key to identify documents; add one and try again.");
        }

        return pkColumns[0];
    }

    private async Task<IReadOnlyList<ColumnInfo>> GetColumnsAsync(
        string resolved,
        string pk,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var columns = new List<ColumnInfo>();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT c.name, c.system_type_id
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@t)
            ORDER BY c.column_id
            """;
        command.Parameters.AddWithValue("@t", resolved);
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            if (string.Equals(name, pk, StringComparison.Ordinal))
            {
                continue;
            }

            columns.Add(new ColumnInfo(name, IsStringType(reader.GetByte(1))));
        }

        return columns;
    }

    private async Task<long> GetCurrentVersionAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is null or DBNull)
        {
            throw new InvalidOperationException(
                "SQL Server change tracking is not enabled on the target database; Pulse could not enable it.");
        }

        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    /// <summary>Returns every change after <paramref name="lastSynced"/>, oldest first.</summary>
    private async Task<IReadOnlyList<(long Version, ChangeEvent Change)>> FetchChangesAsync(
        SqlConnection connection,
        string source,
        string resolved,
        long lastSynced,
        CancellationToken cancellationToken)
    {
        string pk;
        IReadOnlyList<ColumnInfo> columns;
        lock (_sync)
        {
            pk = _pkColumns[resolved];
            columns = _columns[resolved];
        }

        var (schema, table) = ResolveSource(resolved);
        var qualified = $"{QuoteIdent(schema)}.{QuoteIdent(table)}";
        var baseColumns = string.Join(
            ", ",
            columns.Select(c => $"t.{QuoteIdent(c.Name)} AS {QuoteIdent(c.Name)}"));

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT ct.{QuoteIdent(pk)} AS [pulse_pk],
                   ct.SYS_CHANGE_OPERATION AS [pulse_op],
                   ct.SYS_CHANGE_VERSION AS [pulse_version],
                   ct.SYS_CHANGE_COLUMNS AS [pulse_mask],
                   t.{QuoteIdent(pk)} AS [t_pk],
                   {baseColumns}
            FROM CHANGETABLE(CHANGES {qualified}, @lastSynced) AS ct
            LEFT JOIN {qualified} AS t ON t.{QuoteIdent(pk)} = ct.{QuoteIdent(pk)}
            ORDER BY ct.SYS_CHANGE_VERSION
            """;
        command.Parameters.AddWithValue("@lastSynced", lastSynced);

        var rows = new List<(long Version, string Op, byte[]? Mask, string PkValue, IReadOnlyDictionary<string, object?>? Document)>();
        using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var pkValue = Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture) ?? string.Empty;
                var op = reader.GetString(1);
                var version = reader.GetInt64(2);
                var mask = reader.IsDBNull(3) ? null : (byte[])reader[3];
                var document = reader.IsDBNull(4) ? null : ReadDocument(reader, 4, pk, columns);
                rows.Add((version, op, mask, pkValue, document));
            }
        }

        var changes = new List<(long Version, ChangeEvent Change)>(rows.Count);
        var providerId = ProviderIdFor(source);
        var timestamp = DateTimeOffset.UtcNow;
        foreach (var (version, op, mask, pkValue, document) in rows)
        {
            var kind = op switch
            {
                "I" => ChangeKind.Insert,
                "U" => ChangeKind.Update,
                "D" => ChangeKind.Delete,
                _ => throw new InvalidOperationException($"Unknown change operation '{op}' from SQL Server change tracking."),
            };

            // A row that changed but no longer exists (insert/update followed by a delete before
            // this fetch) coalesces to a delete, matching the document being gone.
            if ((kind == ChangeKind.Insert || kind == ChangeKind.Update) && document is null)
            {
                kind = ChangeKind.Delete;
            }

            IReadOnlyDictionary<string, object?>? updatedFields = null;
            if (kind == ChangeKind.Update && mask is not null && document is not null)
            {
                var changed = await DecodeMaskAsync(connection, resolved, mask, cancellationToken).ConfigureAwait(false);
                var fields = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var name in changed)
                {
                    if (document.TryGetValue(name, out var value))
                    {
                        fields[name] = value;
                    }
                }

                updatedFields = fields;
            }

            changes.Add((version, new ChangeEvent(
                Source: source,
                Kind: kind,
                DocumentId: pkValue,
                FullDocument: document,
                UpdatedFields: updatedFields,
                Token: new ResumeToken(providerId, EncodeVersion(version)),
                Timestamp: timestamp)));
        }

        return changes;
    }

    /// <summary>Resolves the columns updated in a change-tracking mask to column names.</summary>
    private async Task<HashSet<string>> DecodeMaskAsync(
        SqlConnection connection,
        string resolved,
        byte[] mask,
        CancellationToken cancellationToken)
    {
        // Masks are bitmaps over the table's own column ordinals, so the same bits decode to
        // different names on different tables — the cache key must be scoped to the table.
        var key = $"{resolved}|{Convert.ToHexString(mask)}";
        lock (_sync)
        {
            if (_maskCache.TryGetValue(key, out var cached))
            {
                return cached;
            }
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@t)
              AND CHANGE_TRACKING_IS_COLUMN_IN_MASK(c.column_id, @mask) = 1
            """;
        command.Parameters.AddWithValue("@t", resolved);
        command.Parameters.Add("@mask", SqlDbType.VarBinary).Value = mask;
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            names.Add(reader.GetString(0));
        }

        lock (_sync)
        {
            _maskCache[key] = names;
        }

        return names;
    }

    private void ValidateProviderId(string source, ResumeToken resumeFrom) => resumeFrom.EnsureProvider(ProviderIdFor(source));


    private async Task ValidateVersionUsableAsync(
        string source,
        string resolved,
        long version,
        CancellationToken cancellationToken)
    {
        try
        {
            var (schema, table) = ResolveSource(resolved);
            var qualified = $"{QuoteIdent(schema)}.{QuoteIdent(table)}";
            string pk;
            lock (_sync)
            {
                pk = _pkColumns[resolved];
            }

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"SELECT TOP (1) ct.{QuoteIdent(pk)} FROM CHANGETABLE(CHANGES {qualified}, @v) AS ct";
            command.Parameters.AddWithValue("@v", version);
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (ex.Number is 22119 or 22122)
        {
            throw new ResumeTokenInvalidException(
                $"Resume token for '{ProviderIdFor(source)}' is older than the change-tracking retention window; its changes were cleaned up. Resync from a fresh snapshot instead.",
                ex);
        }
    }

    /// <summary>Registers a callback on a shared per-source watcher, creating it on first use.</summary>


    private async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>Builds a document dictionary from a reader whose columns start at <paramref name="offset"/>: <c>_id</c> then one column per <see cref="ColumnInfo"/>.</summary>
    private static IReadOnlyDictionary<string, object?> ReadDocument(
        SqlDataReader reader,
        int offset,
        string pk,
        IReadOnlyList<ColumnInfo> columns)
    {
        var document = new Dictionary<string, object?>(columns.Count + 1, StringComparer.Ordinal)
        {
            ["_id"] = CoerceCell(reader.GetValue(offset), isStringType: false),
        };

        for (var i = 0; i < columns.Count; i++)
        {
            document[columns[i].Name] = CoerceCell(reader.GetValue(offset + 1 + i), columns[i].IsStringType);
        }

        return document;
    }

    /// <summary>
    /// Coerces a raw SQL cell into a wire value. String columns that hold a valid JSON object or
    /// array are embedded as nested documents (mirroring <c>jsonb</c> columns on Postgres);
    /// anything else — plain text, scalars, non-string columns — is delivered as-is. Floating
    /// and decimal columns are normalized like JSON numbers (integral values become
    /// <see cref="long"/>, otherwise <see cref="double"/>) so client-side numeric assertions and
    /// the <c>ObjectToInferredTypesConverter</c> agree across providers.
    /// </summary>
    private static object? CoerceCell(object? value, bool isStringType)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        if (isStringType && value is string text)
        {
            try
            {
                using var parsed = JsonDocument.Parse(text);
                return parsed.RootElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                    ? JsonValueConverter.ToClrValue(parsed.RootElement)
                    : text;
            }
            catch (JsonException)
            {
                return text;
            }
        }

        return value switch
        {
            decimal dec => NormalizeNumber(dec),
            double dbl => NormalizeNumber(dbl),
            float flt => NormalizeNumber(flt),
            // uniqueidentifier cells (including the _id / primary key) surface as their string
            // form, matching how JSON serializes them and how every provider's _id is a string
            // in memory — otherwise _id filters never match live change events.
            Guid g => g.ToString(),
            _ => value,
        };
    }

    private const double MaxLongAsDouble = 9223372036854775808.0; // 2^63, exclusive upper bound

    private static object? NormalizeNumber(double value)
        => value == Math.Truncate(value) && value >= long.MinValue && value < MaxLongAsDouble
            ? (object)Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : value;

    private static object? NormalizeNumber(decimal value)
        => value == decimal.Truncate(value) && value >= long.MinValue && value <= long.MaxValue
            ? (object)Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : (object)(double)value;

    private static object? NormalizeNumber(float value)
        => NormalizeNumber((double)value);

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
            return ("dbo", source);
        }

        return (source[..dot], source[(dot + 1)..]);
    }

    private static string ParseDatabaseName(string connectionString)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            var database = builder.InitialCatalog;
            return string.IsNullOrWhiteSpace(database) ? "master" : database;
        }
        catch (ArgumentException)
        {
            return "master";
        }
    }

    private static bool IsStringType(byte systemTypeId)
        => systemTypeId is 35 or 99 or 167 or 175 or 231 or 239; // text/ntext/varchar/char/nvarchar/nchar

    private static string QuoteIdent(string name)
        => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";

    private static byte[] EncodeVersion(long version) => Int64ResumeTokenCodec.Encode(version);
    private static long DecodeVersion(byte[] opaque) => Int64ResumeTokenCodec.Decode(opaque);

    private sealed record ColumnInfo(string Name, bool IsStringType);

    /// <summary>A shared per-source watcher that fans out to multiple subscribers.</summary>


}
