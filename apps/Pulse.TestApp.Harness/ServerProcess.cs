using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Pulse.TestApp.Harness;

/// <summary>
/// Manages the lifecycle of a single Pulse.TestApp.Server process: launch on a free
/// port, wait for /health, capture stdout/stderr to a log, and kill it. The child
/// inherits the harness's environment (connection strings etc.) with the provider
/// overrides applied.
/// </summary>
public sealed class ServerProcess : IAsyncDisposable
{
    private readonly HttpClient _http = new();
    private readonly Process _process;
    private readonly string _logPath;

    public string BaseUrl { get; }
    public string HubUrl => BaseUrl + "/pulse";

    private ServerProcess(string baseUrl, Process process, string logPath)
    {
        BaseUrl = baseUrl;
        _process = process;
        _logPath = logPath;
    }

    public static async Task<ServerProcess> StartAsync(
        string serverDll, string provider, string? resumeTokenDir, CancellationToken ct, string? baseUrl = null)
    {
        baseUrl ??= "http://localhost:" + FreePort();

        var psi = new ProcessStartInfo("dotnet", $"\"{serverDll}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = Path.GetDirectoryName(Path.GetFullPath(serverDll))!,
        };

        psi.Environment["PULSE_PROVIDER"] = provider;
        psi.Environment["PULSE_SERVER_URLS"] = baseUrl;
        psi.Environment["PULSE_RESUME_TOKEN_DIR"] = resumeTokenDir ?? string.Empty;

        var logPath = Path.Combine(
            Path.GetTempPath(), $"pulse-testapp-server-{provider}-{Guid.NewGuid():N}.log");

        var process = Process.Start(psi)
            ?? throw new InvalidOperationException("Failed to start server process.");

        _ = Task.WhenAll(
            DrainAsync(process.StandardOutput, logPath, "OUT"),
            DrainAsync(process.StandardError, logPath, "ERR"));

        var server = new ServerProcess(baseUrl, process, logPath);
        await server.WaitForHealthAsync(ct);
        return server;
    }

    public string LogPath => _logPath;

    public bool HasExited => _process.HasExited;

    private async Task WaitForHealthAsync(CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                using var resp = await _http.GetAsync(BaseUrl + "/health", ct);
                if (resp.IsSuccessStatusCode) return;
            }
            catch
            {
                // server not up yet; keep polling
            }

            if (_process.HasExited)
            {
                throw new InvalidOperationException(
                    $"Server exited early (exit code {_process.ExitCode}). Log: {_logPath}");
            }

            await Task.Delay(250, ct);
        }

        throw new TimeoutException($"Server did not become healthy within 60s. Log: {_logPath}");
    }

    public void Kill()
    {
        try
        {
            if (!_process.HasExited)
            {
                _process.Kill(entireProcessTree: true);
            }
        }
        catch
        {
            // already gone
        }

        try { _process.WaitForExit(); }
        catch { }
    }

    public async ValueTask DisposeAsync()
    {
        Kill();
        await Task.Delay(150).ConfigureAwait(false);
        _http.Dispose();
    }

    private static async Task DrainAsync(StreamReader reader, string logPath, string prefix)
    {
        await using var writer = new StreamWriter(logPath, append: true) { AutoFlush = true };
        while (await reader.ReadLineAsync() is { } line)
        {
            await writer.WriteLineAsync($"[{prefix}] {line}");
        }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}
