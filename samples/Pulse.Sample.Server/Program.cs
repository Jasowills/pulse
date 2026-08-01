using Pulse.Mongo;
using Pulse.Server;

var builder = WebApplication.CreateBuilder(args);

// Persist resume tokens across restarts (opt-in; the default is in-memory).
// Omit this line to keep the default, or point it anywhere writable.
var resumeTokenDir = builder.Configuration["Pulse:ResumeTokenDirectory"];
if (!string.IsNullOrWhiteSpace(resumeTokenDir))
{
    builder.Services.AddSingleton<IResumeTokenStore>(new FileResumeTokenStore(resumeTokenDir));
}

builder.Services.AddMongoSource(options =>
{
    options.ConnectionString = builder.Configuration.GetConnectionString("Mongo")
        ?? "mongodb://localhost:27017";
    options.Database = builder.Configuration["Pulse:Mongo:Database"] ?? "pulse";
});

var app = builder.Build();

app.MapGet("/", () => "Pulse sample server — SignalR hub at /pulse");
app.MapHub<PulseHub>("/pulse");

app.Run();
