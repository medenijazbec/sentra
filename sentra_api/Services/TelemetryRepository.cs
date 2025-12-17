using System.Text.Json;
using Microsoft.Data.Sqlite;
using sentra_api.Models;

namespace sentra_api.Services;

public class TelemetryRepository
{
    private readonly DbOptions _options;
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public TelemetryRepository(DbOptions options)
    {
        _options = options;
    }

    private SqliteConnection OpenConnection()
    {
        var conn = new SqliteConnection($"Data Source={_options.DbPath}");
        conn.Open();
        return conn;
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

        await using (var conn = OpenConnection())
        {
            cpu = await GetLatestCpuAsync(conn, ct);
            mem = await GetLatestMemoryAsync(conn, ct);
            gpus = await GetLatestGpuAsync(conn, ct);
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

        await using (var conn = OpenConnection())
        {
            host = await GetHostHistoryAsync(conn, cutoffEpoch, ct);
            gpus = await GetGpuHistoryAsync(conn, cutoffEpoch, ct);
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

        await using var conn = OpenConnection();
        foreach (var table in tables)
        {
            await using var del = conn.CreateCommand();
            del.CommandText = $"DELETE FROM {table} WHERE ts < $cutoff";
            del.Parameters.AddWithValue("$cutoff", cutoffEpoch);
            await del.ExecuteNonQueryAsync(ct);
        }

        await using var vacuum = conn.CreateCommand();
        vacuum.CommandText = "VACUUM";
        await vacuum.ExecuteNonQueryAsync(ct);
    }

    private async Task<CpuSummary?> GetLatestCpuAsync(SqliteConnection conn, CancellationToken ct)
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
            ReadLong(reader, 7),
            ReadDouble(reader, 8),
            ReadDouble(reader, 9)
        );
    }

    private async Task<MemorySummary?> GetLatestMemoryAsync(SqliteConnection conn, CancellationToken ct)
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

    private async Task<IReadOnlyList<GpuSample>> GetLatestGpuAsync(SqliteConnection conn, CancellationToken ct)
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
            results.Add(new GpuSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                reader.GetInt32(1),
                ReadDouble(reader, 2),
                ReadDouble(reader, 3),
                ReadDouble(reader, 4),
                ReadInt(reader, 5),
                ReadInt(reader, 6),
                ReadDouble(reader, 7)
            ));
        }

        return results;
    }

    private async Task<IReadOnlyList<DiskSample>> GetLatestDiskAsync(SqliteConnection conn, CancellationToken ct)
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

    private async Task<IReadOnlyList<NetSample>> GetLatestNetAsync(SqliteConnection conn, CancellationToken ct)
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

    private async Task<IReadOnlyList<FanSample>> GetLatestFanAsync(SqliteConnection conn, CancellationToken ct)
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

    private async Task<List<HostHistoryPoint>> GetHostHistoryAsync(SqliteConnection conn, long cutoffEpoch, CancellationToken ct)
    {
        var map = new Dictionary<long, HostHistoryPointBuilder>();

        const string cpuSql = """
            SELECT ts, total_util, cpu_temp
            FROM cpu_samples
            WHERE ts >= $cutoff
            ORDER BY ts ASC
            """;

        await using (var cpuCmd = conn.CreateCommand())
        {
            cpuCmd.CommandText = cpuSql;
            cpuCmd.Parameters.AddWithValue("$cutoff", cutoffEpoch);

            await using var reader = await cpuCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var ts = ReadLong(reader, 0);
                if (ts is null) continue;
                if (!map.TryGetValue(ts.Value, out var b))
                {
                    b = new HostHistoryPointBuilder { Ts = ts.Value };
                    map[ts.Value] = b;
                }

                b.CpuPercent = ReadDouble(reader, 1);
                b.CpuTemp = ReadDouble(reader, 2);
            }
        }

        const string memSql = """
            SELECT ts, used_percent, swap_used_percent
            FROM mem_samples
            WHERE ts >= $cutoff
            ORDER BY ts ASC
            """;

        await using (var memCmd = conn.CreateCommand())
        {
            memCmd.CommandText = memSql;
            memCmd.Parameters.AddWithValue("$cutoff", cutoffEpoch);

            await using var reader = await memCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var ts = ReadLong(reader, 0);
                if (ts is null) continue;
                if (!map.TryGetValue(ts.Value, out var b))
                {
                    b = new HostHistoryPointBuilder { Ts = ts.Value };
                    map[ts.Value] = b;
                }

                b.MemoryPercent = ReadDouble(reader, 1);
                b.SwapPercent = ReadDouble(reader, 2);
            }
        }

        return map.Values
            .OrderBy(x => x.Ts)
            .Select(x => x.ToPoint())
            .ToList();
    }

    private async Task<List<GpuSample>> GetGpuHistoryAsync(SqliteConnection conn, long cutoffEpoch, CancellationToken ct)
    {
        const string sql = """
            SELECT ts, gpu_index, temp, util, power_w, vram_used_mb, vram_total_mb, fan_percent
            FROM gpu_samples
            WHERE ts >= $cutoff
            ORDER BY ts ASC, gpu_index ASC
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("$cutoff", cutoffEpoch);

        var results = new List<GpuSample>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var ts = ReadLong(reader, 0) ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            results.Add(new GpuSample(
                DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime,
                reader.GetInt32(1),
                ReadDouble(reader, 2),
                ReadDouble(reader, 3),
                ReadDouble(reader, 4),
                ReadInt(reader, 5),
                ReadInt(reader, 6),
                ReadDouble(reader, 7)
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

    private static double? ReadDouble(SqliteDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetDouble(index);

    private static long? ReadLong(SqliteDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetInt64(index);

    private static int? ReadInt(SqliteDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetInt32(index);

    private static long ToEpoch(DateTime dt) => new DateTimeOffset(dt).ToUnixTimeSeconds();

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
