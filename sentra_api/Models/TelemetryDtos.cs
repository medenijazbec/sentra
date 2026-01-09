namespace sentra_api.Models;

public record DbOptions(string ConnectionString);

public record PurgeRequest(long? CutoffEpoch);

public record TelemetrySummary(
    DateTime Timestamp,
    CpuSummary Cpu,
    MemorySummary Memory,
    IReadOnlyList<GpuSample> Gpus,
    IReadOnlyList<DiskSample> Disks,
    IReadOnlyList<NetSample> Networks,
    IReadOnlyList<FanSample> Fans
);

public record CpuSummary(
    DateTime Timestamp,
    double? TotalUtil,
    double? Temp,
    double? Load1,
    double? Load5,
    double? Load15,
    IReadOnlyList<double> PerCore,
    double? UptimeSec,
    double? UserPct,
    double? SystemPct
);

public record MemorySummary(
    DateTime Timestamp,
    double? UsedPercent,
    long? UsedBytes,
    long? TotalBytes,
    double? SwapUsedPercent
);

public record GpuSample(
    DateTime Timestamp,
    int GpuIndex,
    double? Temp,
    double? Util,
    double? PowerW,
    int? VramUsedMb,
    int? VramTotalMb,
    double? FanPercent,
    string? Label
);

public record DiskSample(
    DateTime Timestamp,
    string Device,
    double? ReadBps,
    double? WriteBps,
    double? UsagePercent
);

public record NetSample(
    DateTime Timestamp,
    string Interface,
    double? RxBps,
    double? TxBps
);

public record FanSample(
    DateTime Timestamp,
    string Label,
    double? Rpm
);

public record FanCurvePoint(
    double TempC,
    int Rpm
);

public record FanProfile(
    string FanId,
    string? DisplayName,
    string Mode,
    int? MaxRpm,
    string CurveMode,
    string? CurveSource,
    IReadOnlyList<FanCurvePoint> Curve,
    IReadOnlyList<int> GpuIndices
);

public record FanSettingsResponse(
    IReadOnlyList<FanProfile> Fans,
    IReadOnlyDictionary<string, double> MaxRecordedRpm
);

public record FanSettingsRequest(
    IReadOnlyList<FanProfile> Fans
);

public record HistoryResponse(
    IReadOnlyList<HostHistoryPoint> Host,
    IReadOnlyList<GpuSample> Gpus
);

public record HostHistoryPoint(
    DateTime Timestamp,
    double? CpuPercent,
    double? CpuTemp,
    double? MemoryPercent,
    double? SwapPercent
);
