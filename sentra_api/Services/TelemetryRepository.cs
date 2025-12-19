using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using sentra_api.Models;

namespace sentra_api.Services;

public class TelemetryRepository
{
    private readonly DbOptions _options;
    private static readonly SemaphoreSlim SchemaLock = new(1, 1);
    private static bool _schemaInitialized;
    private const int DefaultHistoryWindowMinutes = 240;
    private const int MinHistoryWindowMinutes = 15;
    private const int MaxHistoryWindowMinutes = 720;
    private const int DefaultRetentionHours = 720;
    private const int MinRetentionHours = 24;
    private const int MaxRetentionHours = 720;
    private const string HistoryWindowKey = "history_window_minutes";
    private const string RetentionHoursKey = "retention_hours";
    private static readonly TimeSpan GpuStaleThreshold = TimeSpan.FromSeconds(30);
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public TelemetryRepository(DbOptions options)
    {
        _options = options;
    }

    private async Task<MySqlConnection> OpenConnectionAsync()
    {
        var conn = new MySqlConnection(_options.ConnectionString);
        await conn.OpenAsync();
        if (!_schemaInitialized)
        {
            await EnsureSchemaAsync(conn);
        }

        return conn;
    }

    private static async Task EnsureSchemaAsync(MySqlConnection conn)
    {
        if (_schemaInitialized)
        {
            return;
        }

        await SchemaLock.WaitAsync();
        try
        {
            if (_schemaInitialized)
            {
                return;
            }

            var tableStatements = new[]
            {
                """
                CREATE TABLE IF NOT EXISTS cpu_samples (
                    ts BIGINT NOT NULL,
                    total_util DOUBLE,
                    iowait DOUBLE,
                    per_core TEXT,
                    cpu_temp DOUBLE,
                    load1 DOUBLE,
                    load5 DOUBLE,
                    load15 DOUBLE,
                    uptime_sec DOUBLE,
                    user_pct DOUBLE,
                    system_pct DOUBLE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS mem_samples (
                    ts BIGINT NOT NULL,
                    used_percent DOUBLE,
                    used_bytes BIGINT,
                    total_bytes BIGINT,
                    swap_used_percent DOUBLE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS gpu_samples (
                    ts BIGINT NOT NULL,
                    gpu_index INT,
                    temp DOUBLE,
                    util DOUBLE,
                    power_w DOUBLE,
                    vram_used_mb INT,
                    vram_total_mb INT,
                    fan_percent DOUBLE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS disk_samples (
                    ts BIGINT NOT NULL,
                    device TEXT,
                    read_bps DOUBLE,
                    write_bps DOUBLE,
                    usage_percent DOUBLE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS net_samples (
                    ts BIGINT NOT NULL,
                    iface TEXT,
                    rx_bps DOUBLE,
                    tx_bps DOUBLE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS fan_samples (
                    ts BIGINT NOT NULL,
                    label TEXT,
                    rpm DOUBLE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS dashboard_settings (
                    `key` VARCHAR(128) PRIMARY KEY,
                    `value` TEXT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS gpu_labels (
                    gpu_index INT PRIMARY KEY,
                    label TEXT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                """
                CREATE TABLE IF NOT EXISTS gpu_visibility (
                    gpu_index INT PRIMARY KEY,
                    hidden TINYINT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            };

            foreach (var sql in tableStatements)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync();
            }

            var indexStatements = new[]
            {
                "CREATE INDEX idx_cpu_ts ON cpu_samples(ts)",
                "CREATE INDEX idx_mem_ts ON mem_samples(ts)",
                "CREATE INDEX idx_gpu_ts ON gpu_samples(ts)",
                "CREATE INDEX idx_disk_ts ON disk_samples(ts)",
                "CREATE INDEX idx_net_ts ON net_samples(ts)",
                "CREATE INDEX idx_fan_ts ON fan_samples(ts)"
            };

            foreach (var sql in indexStatements)
            {
                await using var idxCmd = conn.CreateCommand();
                idxCmd.CommandText = sql;
                try
                {
                    await idxCmd.ExecuteNonQueryAsync();
                }
                catch (MySqlException ex) when (ex.Number == 1061)
                {
                    // Index already exists; ignore
                }
            }

            _schemaInitialized = true;
        }
        finally
        {
            SchemaLock.Release();
        }
    }

    public async Task<int> GetHistoryWindowAsync(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync();
        var raw = await ReadSettingValueAsync(conn, HistoryWindowKey, ct);
        if (int.TryParse(raw, out var parsed))
        {
            return Clamp(parsed, MinHistoryWindowMinutes, MaxHistoryWindowMinutes);
        }

        return DefaultHistoryWindowMinutes;
    }

    public async Task SetHistoryWindowAsync(int minutes, CancellationToken ct)
    {
        var normalized = Clamp(minutes, MinHistoryWindowMinutes, MaxHistoryWindowMinutes).ToString();
        await using var conn = await OpenConnectionAsync();
        await SetSettingValueAsync(conn, HistoryWindowKey, normalized, ct);
    }

    public async Task<int> GetRetentionHoursAsync(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync();
        var raw = await ReadSettingValueAsync(conn, RetentionHoursKey, ct);
        if (int.TryParse(raw, out var parsed))
        {
            return Clamp(parsed, MinRetentionHours, MaxRetentionHours);
        }

        return DefaultRetentionHours;
    }

    public async Task SetRetentionHoursAsync(int hours, CancellationToken ct)
    {
        var normalized = Clamp(hours, MinRetentionHours, MaxRetentionHours).ToString();
        await using var conn = await OpenConnectionAsync();
        await SetSettingValueAsync(conn, RetentionHoursKey, normalized, ct);
    }

    public async Task<Dictionary<int, string>> GetGpuLabelsAsync(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync();
        return await ReadGpuLabelsAsync(conn, ct);
    }

    public async Task SetGpuLabelAsync(int gpuIndex, string? label, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync();
        if (string.IsNullOrWhiteSpace(label))
        {
            await using var delete = conn.CreateCommand();
            delete.CommandText = "DELETE FROM gpu_labels WHERE gpu_index = @idx";
            delete.Parameters.AddWithValue("@idx", gpuIndex);
            await ExecuteNonQueryWithRetryAsync(delete, ct);
            return;
        }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO gpu_labels (gpu_index, label)
            VALUES (@idx, @label)
            ON DUPLICATE KEY UPDATE label = VALUES(label)
            """;
        cmd.Parameters.AddWithValue("@idx", gpuIndex);
        cmd.Parameters.AddWithValue("@label", label.Trim());
        await ExecuteNonQueryWithRetryAsync(cmd, ct);
    }

    public async Task<Dictionary<int, bool>> GetGpuVisibilityAsync(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync();
        return await ReadGpuVisibilityAsync(conn, ct);
    }

    public async Task SetGpuVisibilityAsync(int gpuIndex, bool hidden, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync();
        if (!hidden)
        {
            await using var delete = conn.CreateCommand();
            delete.CommandText = "DELETE FROM gpu_visibility WHERE gpu_index = @idx";
            delete.Parameters.AddWithValue("@idx", gpuIndex);
            await ExecuteNonQueryWithRetryAsync(delete, ct);
            return;
        }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO gpu_visibility (gpu_index, hidden)
            VALUES (@idx, 1)
            ON DUPLICATE KEY UPDATE hidden = VALUES(hidden)
            """;
        cmd.Parameters.AddWithValue("@idx", gpuIndex);
        await ExecuteNonQueryWithRetryAsync(cmd, ct);
    }

    private static int Clamp(int value, int min, int max) => Math.Max(min, Math.Min(max, value));

    private static async Task<string?> ReadSettingValueAsync(MySqlConnection conn, string key, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT `value` FROM dashboard_settings WHERE `key` = @key";
        cmd.Parameters.AddWithValue("@key", key);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result as string;
    }

    private static async Task SetSettingValueAsync(MySqlConnection conn, string key, string value, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO dashboard_settings (`key`, `value`)
            VALUES (@key, @value)
            ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)
            """;
        cmd.Parameters.AddWithValue("@key", key);
        cmd.Parameters.AddWithValue("@value", value);
        await ExecuteNonQueryWithRetryAsync(cmd, ct);
    }

    private static async Task<Dictionary<int, string>> ReadGpuLabelsAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT gpu_index, label
            FROM gpu_labels
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var map = new Dictionary<int, string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var idx = reader.GetInt32(0);
            var label = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (!string.IsNullOrWhiteSpace(label))
            {
                map[idx] = label!;
            }
        }

        return map;
    }

    private static async Task<Dictionary<int, bool>> ReadGpuVisibilityAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT gpu_index, hidden
            FROM gpu_visibility
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var map = new Dictionary<int, bool>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var idx = reader.GetInt32(0);
            var hidden = reader.GetInt32(1) != 0;
            map[idx] = hidden;
        }

        return map;
    }

    public async Task<TelemetrySummary?> GetSummaryAsync(CancellationToken ct)
    {
        CpuSummary? cpu = null;
        MemorySummary? mem = null;
        IReadOnlyList<GpuSample> gpus = Array.Empty<GpuSample>();
        IReadOnlyList<DiskSample> disks = Array.Empty<DiskSample>();
        IReadOnlyList<NetSample> nets = Array.Empty<NetSample>();
        IReadOnlyList<FanSample> fans = Array.Empty<FanSample>();
        long latestTs = 0;

        await using (var conn = await OpenConnectionAsync())
        {
            cpu = await GetLatestCpuAsync(conn, ct);
            mem = await GetLatestMemoryAsync(conn, ct);
            var gpuLabels = await ReadGpuLabelsAsync(conn, ct);
            gpus = await GetLatestGpuAsync(conn, ct, gpuLabels);
            gpus = PruneStaleGpuSamples(gpus);
            disks = await GetLatestDiskAsync(conn, ct);
            nets = await GetLatestNetAsync(conn, ct);
            fans = await GetLatestFanAsync(conn, ct);
        }

        long MaxTs(long current, IEnumerable<long> list) =>
            Math.Max(current, list.DefaultIfEmpty(0).Max());

        if (cpu is not null)
        {
            latestTs = Math.Max(latestTs, ToEpoch(cpu.Timestamp));
        }

        if (mem is not null)
        {
            latestTs = Math.Max(latestTs, ToEpoch(mem.Timestamp));
        }

        latestTs = MaxTs(latestTs, gpus.Select(g => ToEpoch(g.Timestamp)));
        latestTs = MaxTs(latestTs, disks.Select(d => ToEpoch(d.Timestamp)));
        latestTs = MaxTs(latestTs, nets.Select(n => ToEpoch(n.Timestamp)));
        latestTs = MaxTs(latestTs, fans.Select(f => ToEpoch(f.Timestamp)));

        if (cpu is null && mem is null)
        {
            return null;
        }

        var timestamp = latestTs == 0
            ? DateTime.UtcNow
            : DateTimeOffset.FromUnixTimeSeconds(latestTs).UtcDateTime;

        return new TelemetrySummary(
            timestamp,
            cpu ?? new CpuSummary(DateTime.UtcNow, null, null, null, null, null, Array.Empty<double>(), null, null, null),
            mem ?? new MemorySummary(DateTime.UtcNow, null, null, null, null),
            gpus,
            disks,
            nets,
            fans
        );
    }

    public async Task<HistoryResponse> GetHistoryAsync(int minutes, CancellationToken ct)
    {
        var cutoffEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - (minutes * 60);
        List<HostHistoryPoint> host;
        List<GpuSample> gpus;

        await using (var conn = await OpenConnectionAsync())
        {
            host = await GetHostHistoryAsync(conn, cutoffEpoch, ct);
            var gpuLabels = await ReadGpuLabelsAsync(conn, ct);
            gpus = await GetGpuHistoryAsync(conn, cutoffEpoch, ct, gpuLabels);
        }

        return new HistoryResponse(host, gpus);
    }

    public async Task PurgeBeforeAsync(long cutoffEpoch, CancellationToken ct)
    {
        var tables = new[]
        {
            "cpu_samples",
            "mem_samples",
            "gpu_samples",
            "disk_samples",
            "net_samples",
            "fan_samples"
        };

        await using var conn = await OpenConnectionAsync();
        foreach (var table in tables)
        {
            await using var del = conn.CreateCommand();
            del.CommandText = $"DELETE FROM {table} WHERE ts < @cutoff";
            del.Parameters.AddWithValue("@cutoff", cutoffEpoch);
            await ExecuteNonQueryWithRetryAsync(del, ct);
        }
    }

    public async Task PurgeAllAsync(CancellationToken ct)
    {
        var tables = new[]
        {
            "cpu_samples",
            "mem_samples",
            "gpu_samples",
            "disk_samples",
            "net_samples",
            "fan_samples"
        };

        await using var conn = await OpenConnectionAsync();
        foreach (var table in tables)
        {
            await using var del = conn.CreateCommand();
            del.CommandText = $"DELETE FROM {table}";
            await ExecuteNonQueryWithRetryAsync(del, ct);
        }
    }

    private async Task<CpuSummary?> GetLatestCpuAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT ts, total_util, per_core, cpu_temp, load1, load5, load15, uptime_sec, user_pct, system_pct
            FROM cpu_samples
            ORDER BY ts DESC
            LIMIT 1
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var perCoreRaw = reader.IsDBNull(2) ? "[]" : reader.GetString(2);
        var perCore = ParsePerCore(perCoreRaw);

        return new CpuSummary(
            DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
            ReadDouble(reader, 1),
            ReadDouble(reader, 3),
            ReadDouble(reader, 4),
            ReadDouble(reader, 5),
            ReadDouble(reader, 6),
            perCore,
            ReadDouble(reader, 7),
            ReadDouble(reader, 8),
            ReadDouble(reader, 9)
        );
    }

    private async Task<MemorySummary?> GetLatestMemoryAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT ts, used_percent, used_bytes, total_bytes, swap_used_percent
            FROM mem_samples
            ORDER BY ts DESC
            LIMIT 1
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return new MemorySummary(
            DateTimeOffset.FromUnixTimeSeconds(ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds()).UtcDateTime,
            ReadDouble(reader, 1),
            ReadLong(reader, 2),
            ReadLong(reader, 3),
            ReadDouble(reader, 4)
        );
    }

    private async Task<IReadOnlyList<GpuSample>> GetLatestGpuAsync(
        MySqlConnection conn,
        CancellationToken ct,
        IReadOnlyDictionary<int, string>? gpuLabels = null)
    {
        const string sql = """
            WITH ranked AS (
                SELECT ts, gpu_index, temp, util, power_w, vram_used_mb, vram_total_mb, fan_percent,
                       ROW_NUMBER() OVER (PARTITION BY gpu_index ORDER BY ts DESC) AS rn
                FROM gpu_samples
            )
            SELECT ts, gpu_index, temp, util, power_w, vram_used_mb, vram_total_mb, fan_percent
            FROM ranked
            WHERE rn = 1
            ORDER BY gpu_index ASC
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var results = new List<GpuSample>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var gpuIndex = reader.GetInt32(1);
            var label = gpuLabels != null && gpuLabels.TryGetValue(gpuIndex, out var named) ? named : null;

            results.Add(new GpuSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                gpuIndex,
                ReadDouble(reader, 2),
                ReadDouble(reader, 3),
                ReadDouble(reader, 4),
                ReadInt(reader, 5),
                ReadInt(reader, 6),
                ReadDouble(reader, 7),
                label
            ));
        }

        return results;
    }

    private static IReadOnlyList<GpuSample> PruneStaleGpuSamples(IReadOnlyList<GpuSample> samples)
    {
        if (samples.Count == 0)
        {
            return samples;
        }

        var cutoff = DateTime.UtcNow - GpuStaleThreshold;
        var filtered = samples.Where(g => g.Timestamp >= cutoff).ToArray();
        return filtered.Length == samples.Count ? samples : filtered;
    }

    private async Task<IReadOnlyList<DiskSample>> GetLatestDiskAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            WITH ranked AS (
                SELECT ts, device, read_bps, write_bps, usage_percent,
                       ROW_NUMBER() OVER (PARTITION BY device ORDER BY ts DESC) AS rn
                FROM disk_samples
            )
            SELECT ts, device, read_bps, write_bps, usage_percent
            FROM ranked
            WHERE rn = 1
            ORDER BY device ASC
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var results = new List<DiskSample>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            results.Add(new DiskSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                reader.GetString(1),
                ReadDouble(reader, 2),
                ReadDouble(reader, 3),
                ReadDouble(reader, 4)
            ));
        }

        return results;
    }

    private async Task<IReadOnlyList<NetSample>> GetLatestNetAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            WITH ranked AS (
                SELECT ts, iface, rx_bps, tx_bps,
                       ROW_NUMBER() OVER (PARTITION BY iface ORDER BY ts DESC) AS rn
                FROM net_samples
            )
            SELECT ts, iface, rx_bps, tx_bps
            FROM ranked
            WHERE rn = 1
            ORDER BY iface ASC
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var results = new List<NetSample>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            results.Add(new NetSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                reader.GetString(1),
                ReadDouble(reader, 2),
                ReadDouble(reader, 3)
            ));
        }

        return results;
    }

    private async Task<IReadOnlyList<FanSample>> GetLatestFanAsync(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            WITH ranked AS (
                SELECT ts, label, rpm,
                       ROW_NUMBER() OVER (PARTITION BY label ORDER BY ts DESC) AS rn
                FROM fan_samples
            )
            SELECT ts, label, rpm
            FROM ranked
            WHERE rn = 1
            ORDER BY label ASC
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var results = new List<FanSample>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            results.Add(new FanSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                reader.GetString(1),
                ReadDouble(reader, 2)
            ));
        }

        return results;
    }

    private async Task<List<HostHistoryPoint>> GetHostHistoryAsync(MySqlConnection conn, long cutoffEpoch, CancellationToken ct)
    {
        var map = new Dictionary<long, HostHistoryPointBuilder>();

        const string cpuSql = """
            SELECT ts, total_util, cpu_temp
            FROM cpu_samples
            WHERE ts >= @cutoff
            ORDER BY ts ASC
            """;

        await using (var cpuCmd = conn.CreateCommand())
        {
            cpuCmd.CommandText = cpuSql;
            cpuCmd.Parameters.AddWithValue("@cutoff", cutoffEpoch);

            await using var reader = await cpuCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var ts = ReadLong(reader, 0);
                if (ts is null)
                {
                    continue;
                }

                if (!map.TryGetValue(ts.Value, out var builder))
                {
                    builder = new HostHistoryPointBuilder { Ts = ts.Value };
                    map[ts.Value] = builder;
                }

                builder.CpuPercent = ReadDouble(reader, 1);
                builder.CpuTemp = ReadDouble(reader, 2);
            }
        }

        const string memSql = """
            SELECT ts, used_percent, swap_used_percent
            FROM mem_samples
            WHERE ts >= @cutoff
            ORDER BY ts ASC
            """;

        await using (var memCmd = conn.CreateCommand())
        {
            memCmd.CommandText = memSql;
            memCmd.Parameters.AddWithValue("@cutoff", cutoffEpoch);

            await using var reader = await memCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var ts = ReadLong(reader, 0);
                if (ts is null)
                {
                    continue;
                }

                if (!map.TryGetValue(ts.Value, out var builder))
                {
                    builder = new HostHistoryPointBuilder { Ts = ts.Value };
                    map[ts.Value] = builder;
                }

                builder.MemoryPercent = ReadDouble(reader, 1);
                builder.SwapPercent = ReadDouble(reader, 2);
            }
        }

        return map.Values
            .OrderBy(x => x.Ts)
            .Select(x => x.ToPoint())
            .ToList();
    }

    private async Task<List<GpuSample>> GetGpuHistoryAsync(
        MySqlConnection conn,
        long cutoffEpoch,
        CancellationToken ct,
        IReadOnlyDictionary<int, string>? gpuLabels = null)
    {
        const string sql = """
            SELECT ts, gpu_index, temp, util, power_w, vram_used_mb, vram_total_mb, fan_percent
            FROM gpu_samples
            WHERE ts >= @cutoff
            ORDER BY ts ASC, gpu_index ASC
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@cutoff", cutoffEpoch);

        var results = new List<GpuSample>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var gpuIndex = reader.GetInt32(1);
            var label = gpuLabels != null && gpuLabels.TryGetValue(gpuIndex, out var named) ? named : null;

            results.Add(new GpuSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                gpuIndex,
                ReadDouble(reader, 2),
                ReadDouble(reader, 3),
                ReadDouble(reader, 4),
                ReadInt(reader, 5),
                ReadInt(reader, 6),
                ReadDouble(reader, 7),
                label
            ));
        }

        return results;
    }

    private static IReadOnlyList<double> ParsePerCore(string json)
    {
        try
        {
            var arr = JsonSerializer.Deserialize<List<double>>(json, JsonOpts);
            return arr?.Select(x => (double)x).ToArray() ?? Array.Empty<double>();
        }
        catch
        {
            return Array.Empty<double>();
        }
    }

    private static double? ReadDouble(MySqlDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetDouble(index);

    private static long? ReadLong(MySqlDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetInt64(index);

    private static int? ReadInt(MySqlDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetInt32(index);

    private static long ToEpoch(DateTime dt) => new DateTimeOffset(dt).ToUnixTimeSeconds();

    private static async Task ExecuteNonQueryWithRetryAsync(MySqlCommand cmd, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
                return;
            }
            catch (MySqlException ex) when (IsLockError(ex))
            {
                if (DateTime.UtcNow >= deadline)
                {
                    throw;
                }

                await Task.Delay(120, ct);
            }
        }
    }

    private static bool IsLockError(MySqlException ex) =>
        ex.Number == 1205 || // Lock wait timeout exceeded
        ex.Number == 1213;   // Deadlock found

    private sealed class HostHistoryPointBuilder
    {
        public long Ts { get; set; }
        public double? CpuPercent { get; set; }
        public double? CpuTemp { get; set; }
        public double? MemoryPercent { get; set; }
        public double? SwapPercent { get; set; }

        public HostHistoryPoint ToPoint() => new(
            DateTimeOffset.FromUnixTimeSeconds(Ts).UtcDateTime,
            CpuPercent,
            CpuTemp,
            MemoryPercent,
            SwapPercent
        );
    }
}
