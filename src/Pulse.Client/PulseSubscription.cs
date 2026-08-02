using System.Text.Json;
using System.Threading.Channels;
using Pulse.Abstractions;

namespace Pulse.Client;

/// <summary>
/// Receives wire messages from the owning <see cref="PulseClient"/> and applies them, in
/// order, to <see cref="Current"/> and the <see cref="OnSnapshot"/>/<see cref="OnChange"/>
/// events. All state mutations happen on a single consumer task so snapshots and changes
/// can never be applied out of order, no matter which thread enqueued them.
/// </summary>
internal sealed class PulseSubscription<T> : IPulseSubscription<T>, IPulseSubscriptionHost
{
    private readonly Channel<object> _inbox = Channel.CreateUnbounded<object>();
    private readonly Task _processor;
    private readonly JsonSerializerOptions _json;
    private readonly Func<string, Task> _unsubscribe;
    private readonly Dictionary<string, T> _documents = new(StringComparer.Ordinal);

    public string Id { get; }

    public string Source { get; }

    public event Action<IReadOnlyList<T>>? OnSnapshot;

    public event Action<PulseChange<T>>? OnChange;

    internal PulseSubscription(string id, string source, JsonSerializerOptions json, Func<string, Task> unsubscribe)
    {
        Id = id;
        Source = source;
        _json = json;
        _unsubscribe = unsubscribe;
        _processor = Task.Run(ProcessAsync);
    }

    public IReadOnlyList<T> Current
    {
        get
        {
            lock (_documents)
            {
                return _documents.Values.ToArray();
            }
        }
    }

    public Task UnsubscribeAsync(CancellationToken cancellationToken = default)
        => _unsubscribe(Id);

    /// <summary>Enqueues a wire message for in-order processing. Safe to call from any thread.</summary>
    public void Enqueue(object message)
        => _inbox.Writer.TryWrite(message);

    /// <summary>Stops the consumer loop; no further events fire.</summary>
    public void Close()
        => _inbox.Writer.TryComplete();

    /// <summary>Returns when everything enqueued before this call has been processed.</summary>
    internal Task WaitForIdleAsync()
    {
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_inbox.Writer.TryWrite(signal))
        {
            signal.TrySetResult(true);
        }

        return signal.Task;
    }

    private async Task ProcessAsync()
    {
        await foreach (var message in _inbox.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                switch (message)
                {
                    case TaskCompletionSource<bool> signal:
                        signal.TrySetResult(true);
                        break;
                    case PulseSnapshotMessage snapshot:
                        ApplySnapshot(snapshot);
                        break;
                    case PulseChangeMessage change:
                        ApplyChange(change);
                        break;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Pulse subscription '{Id}' failed to process a message: {ex}");
            }
        }
    }

    private void ApplySnapshot(PulseSnapshotMessage snapshot)
    {
        var docs = new List<T>(snapshot.Documents.Count);
        var byId = new Dictionary<string, T>(StringComparer.Ordinal);
        foreach (var raw in snapshot.Documents)
        {
            var document = ToDocument(raw);
            docs.Add(document);
            if (TryGetId(raw, out var id))
            {
                byId[id] = document;
            }
        }

        lock (_documents)
        {
            _documents.Clear();
            foreach (var (id, document) in byId)
            {
                _documents[id] = document;
            }
        }

        OnSnapshot?.Invoke(docs);
    }

    private void ApplyChange(PulseChangeMessage change)
    {
        var document = change.Kind == ChangeKind.Delete ? default : ToDocument(change.Document);
        switch (change.Kind)
        {
            case ChangeKind.Delete:
                lock (_documents)
                {
                    _documents.Remove(change.DocumentId);
                }

                break;
            case ChangeKind.Insert:
            case ChangeKind.Update:
            case ChangeKind.Replace:
                if (document is not null)
                {
                    lock (_documents)
                    {
                        _documents[change.DocumentId] = document;
                    }
                }

                break;
        }

        OnChange?.Invoke(new PulseChange<T>(
            change.Kind, change.DocumentId, document, change.UpdatedFields, change.Timestamp));
    }

    private static bool TryGetId(IReadOnlyDictionary<string, object?> raw, out string id)
    {
        if (raw.TryGetValue("_id", out var value) && value is not null)
        {
            id = value.ToString()!;
            return id.Length > 0;
        }

        id = string.Empty;
        return false;
    }

    private T ToDocument(IReadOnlyDictionary<string, object?>? raw)
    {
        if (raw is null)
        {
            return default!;
        }

        var json = JsonSerializer.Serialize(raw, _json);
        return JsonSerializer.Deserialize<T>(json, _json)!;
    }
}
