namespace Pulse.TestApp.Core;

public enum ProviderKind
{
    Mongo,
    Postgres,
    SqlServer,
}

public static class TestAppConfig
{
    public const string DefaultMongoUri = "mongodb://localhost:27017";
    public const string DefaultMongoDatabase = "pulse";
    public const string DefaultPostgres = "Host=localhost;Port=5432;Database=pulse;Username=postgres;Password=postgres";
    public const string DefaultSqlServer =
        "Server=localhost,1433;Database=pulse;User Id=sa;Password=PulseTestApp!2026;TrustServerCertificate=True;Encrypt=Optional";
    public const string DefaultHubUrl = "http://localhost:5210/pulse";
    public const string DefaultResumeTokenDirectory = "/tmp/pulse-resume-tokens";

    public static string MongoUri => Env("PULSE_MONGO_URI", DefaultMongoUri);

    public static string MongoDatabase => Env("PULSE_MONGO_DB", DefaultMongoDatabase);

    public static string PostgresConnectionString => Env("PULSE_POSTGRES", DefaultPostgres);

    public static string SqlServerConnectionString => Env("PULSE_SQLSERVER", DefaultSqlServer);

    public static string HubUrl => Env("PULSE_HUB_URL", DefaultHubUrl);

    /// <summary>When set, the test server uses a file-backed resume-token store.</summary>
    public static string? ResumeTokenDirectory => EnvOrNull("PULSE_RESUME_TOKEN_DIR");

    public static ProviderKind ParseProvider(string value)
        => (value ?? string.Empty).Trim().ToLowerInvariant() switch
        {
            "mongo" => ProviderKind.Mongo,
            "postgres" => ProviderKind.Postgres,
            "sqlserver" => ProviderKind.SqlServer,
            _ => throw new ArgumentException($"Unknown provider '{value}'. Expected mongo|postgres|sqlserver."),
        };

    public static string ProviderName(ProviderKind kind)
        => kind switch
        {
            ProviderKind.Mongo => "mongo",
            ProviderKind.Postgres => "postgres",
            ProviderKind.SqlServer => "sqlserver",
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

    private static string Env(string name, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    private static string? EnvOrNull(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}