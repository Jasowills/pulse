namespace Pulse.TestApp;

/// <summary>
/// Environment-driven app configuration. Override the hub URL / provider label via
/// the matching environment variables when running against the local test server.
/// </summary>
public static class AppConfig
{
    public static string HubUrl =>
        Environment.GetEnvironmentVariable("PULSE_HUB_URL") ?? "http://localhost:5210/pulse";

    /// <summary>Display-only label for the provider the test server is running against.</summary>
    public static string ProviderLabel =>
        Environment.GetEnvironmentVariable("PULSE_PROVIDER") ?? "mongo";
}