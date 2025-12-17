using Microsoft.OpenApi.Models;
using sentra_api.Models;
using sentra_api.Services;
using SQLitePCL;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddEnvironmentVariables();

// Ensure SQLite provider is wired up before any connections open.
Batteries_V2.Init();

var dbPath = ResolveDbPath(builder.Configuration);
builder.Services.AddSingleton(new DbOptions(dbPath));
builder.Services.AddScoped<TelemetryRepository>();

builder.Services.AddCors(opt =>
{
    opt.AddPolicy("ui", p =>
    {
        var envOrigins = builder.Configuration["SENTRA_UI_ORIGINS"] ??
                         builder.Configuration["Cors:Origins"] ??
                         "";
        var origins = envOrigins
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (origins.Length == 0)
        {
            // permissive fallback to avoid CORS issues on LAN hosts/ports
            p.AllowAnyOrigin()
             .AllowAnyHeader()
             .AllowAnyMethod();
            return;
        }

        p.WithOrigins(origins)
         .AllowAnyHeader()
         .AllowAnyMethod();
    });
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(opt =>
{
    opt.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "sentra API",
        Version = "v1",
        Description = "Read-only API for sentra host telemetry stored in SQLite."
    });
});

var app = builder.Build();

app.UseCors("ui");

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapGet("/api/health", () =>
{
    return Results.Ok(new
    {
        status = "ok",
        time = DateTimeOffset.UtcNow,
        database = dbPath
    });
});

app.MapGet("/api/telemetry/summary", async (TelemetryRepository repo, CancellationToken ct) =>
{
    var summary = await repo.GetSummaryAsync(ct);
    return summary is null ? Results.NotFound() : Results.Ok(summary);
});

app.MapGet("/api/telemetry/history", async (TelemetryRepository repo, int minutes, CancellationToken ct) =>
{
    var window = minutes <= 0 ? 60 : minutes;
    var history = await repo.GetHistoryAsync(window, ct);
    return Results.Ok(history);
});

app.MapPost("/api/telemetry/purge", async (TelemetryRepository repo, PurgeRequest req, CancellationToken ct) =>
{
    var cutoff = req.CutoffEpoch ?? DateTimeOffset.UtcNow.AddDays(-1).ToUnixTimeSeconds();
    await repo.PurgeBeforeAsync(cutoff, ct);
    return Results.Ok(new { purged_before = cutoff });
});

app.Run();

static string ResolveDbPath(IConfiguration config)
{
    var path = Environment.GetEnvironmentVariable("SENTRA_DB_PATH")
               ?? config["SENTRA_DB_PATH"]
               ?? "/data/sentra.db";

    var normalized = path.Replace("\\", "/");
    if (normalized.StartsWith("/data", StringComparison.OrdinalIgnoreCase) &&
        !Directory.Exists("/data"))
    {
        path = Path.Combine(AppContext.BaseDirectory, "sentra.db");
    }

    if (!Path.IsPathRooted(path))
    {
        path = Path.Combine(AppContext.BaseDirectory, path);
    }

    var dir = Path.GetDirectoryName(path);
    if (!string.IsNullOrWhiteSpace(dir))
    {
        Directory.CreateDirectory(dir);
    }

    return path;
}
