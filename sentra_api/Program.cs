using System;
using System.Diagnostics;
using Microsoft.OpenApi.Models;
using MySqlConnector;
using sentra_api.Models;
using sentra_api.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddEnvironmentVariables();

var dbConnectionString = ResolveDbConnectionString(builder.Configuration);
var dbSummary = BuildDbSummary(dbConnectionString);
builder.Services.AddSingleton(new DbOptions(dbConnectionString));
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
        Description = "Read-only API for sentra host telemetry stored in MySQL."
    });
});

var app = builder.Build();

if (OperatingSystem.IsWindows())
{
    try
    {
        Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;
    }
    catch
    {
        // best effort
    }
}

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
        database = dbSummary
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

static string ResolveDbConnectionString(IConfiguration config)
{
    var envUrl = Environment.GetEnvironmentVariable("SENTRA_DB_URL") ??
                 config["SENTRA_DB_URL"] ??
                 string.Empty;

    MySqlConnectionStringBuilder builder;
    if (!string.IsNullOrWhiteSpace(envUrl))
    {
        builder = new MySqlConnectionStringBuilder(envUrl);
    }
    else
    {
        var host = Environment.GetEnvironmentVariable("SENTRA_DB_HOST") ??
                   config["SENTRA_DB_HOST"] ??
                   "mysql";
        var port = Environment.GetEnvironmentVariable("SENTRA_DB_PORT") ??
                   config["SENTRA_DB_PORT"] ??
                   "3306";
        var user = Environment.GetEnvironmentVariable("SENTRA_DB_USER") ??
                   config["SENTRA_DB_USER"] ??
                   "root";
        var password = Environment.GetEnvironmentVariable("SENTRA_DB_PASSWORD") ??
                       config["SENTRA_DB_PASSWORD"] ??
                       "root";
        var database = Environment.GetEnvironmentVariable("SENTRA_DB_NAME") ??
                       config["SENTRA_DB_NAME"] ??
                       "sentra";

        builder = new MySqlConnectionStringBuilder
        {
            Server = host,
            Port = uint.TryParse(port, out var parsed) ? parsed : 3306,
            UserID = user,
            Password = password,
            Database = database
        };
    }

    builder.SslMode = MySqlSslMode.None;
    builder.CharacterSet = "utf8mb4";
    builder.AllowPublicKeyRetrieval = true;
    builder.AllowUserVariables = true;
    builder.Pooling = true;
    builder.ConnectionTimeout = 30;
    builder.DefaultCommandTimeout = 45;
    builder.ConvertZeroDateTime = true;
    return builder.ConnectionString;
}

static string BuildDbSummary(string connectionString)
{
    var builder = new MySqlConnectionStringBuilder(connectionString);
    var database = string.IsNullOrWhiteSpace(builder.Database) ? "sentra" : builder.Database;
    return $"{builder.UserID}@{builder.Server}:{builder.Port}/{database}";
}
