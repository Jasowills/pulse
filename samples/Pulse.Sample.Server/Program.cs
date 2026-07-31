using Pulse.Mongo;
using Pulse.Server;

var builder = WebApplication.CreateBuilder(args);

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
