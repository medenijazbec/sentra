import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getFanSettings, getHistory, getSummary, saveFanSettings } from "./api/client";
import {
  FanCurvePoint,
  FanProfile,
  FanSettingsResponse,
  GpuSample,
  HistoryResponse,
  NetSample,
  TelemetrySummary,
} from "./types/telemetry";
import { StatCard } from "./components/StatCard";
import { Sparkline } from "./components/Sparkline";
import { FanGauge } from "./components/FanGauge";
import { MiniTrend } from "./components/MiniTrend";
import { fmtBps, fmtBytes, fmtDuration, fmtPct, fmtTemp } from "./lib/format";

export default function App() {
  const DefaultFanMaxRpm = 4000;
  const [summary, setSummary] = useState<TelemetrySummary | null>(null);
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [minutes, setMinutes] = useState(240);
  const [refreshMs, setRefreshMs] = useState(5000);
  const [loading, setLoading] = useState(true);
  const [theme, setTheme] = useState<"phosphor" | "night">("phosphor");
  const [fanSettings, setFanSettings] = useState<FanSettingsResponse | null>(null);
  const [fanDrafts, setFanDrafts] = useState<Record<string, FanProfile>>({});
  const [fanDraftsLoaded, setFanDraftsLoaded] = useState(false);
  const [fanSaving, setFanSaving] = useState(false);
  const [fanSaveStatus, setFanSaveStatus] = useState<string | null>(null);
  const [fanDirty, setFanDirty] = useState(false);
  const [fanSettingsOpenId, setFanSettingsOpenId] = useState<string | null>(null);

  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove("theme-night", "theme-phosphor");
    root.classList.add(theme === "night" ? "theme-night" : "theme-phosphor");
  }, [theme]);

  const defaultFanCurve = useCallback((maxRpm: number): FanCurvePoint[] => {
    const safeMax = Math.max(maxRpm, 1000);
    return [
      { tempC: 30, rpm: Math.round(safeMax * 0.3) },
      { tempC: 45, rpm: Math.round(safeMax * 0.45) },
      { tempC: 60, rpm: Math.round(safeMax * 0.65) },
      { tempC: 75, rpm: Math.round(safeMax * 0.85) },
      { tempC: 90, rpm: safeMax },
    ];
  }, []);

  const buildDefaultFanProfile = useCallback(
    (fanId: string, maxRpm: number): FanProfile => ({
      fanId,
      displayName: "",
      mode: "default",
      maxRpm,
      curveMode: "custom",
      curveSource: "cpu",
      curve: defaultFanCurve(maxRpm),
      gpuIndices: [],
    }),
    [defaultFanCurve]
  );

  useEffect(() => {
    let cancelled = false;

    async function loadFanSettings() {
      try {
        const settings = await getFanSettings();
        if (cancelled) return;
        setFanSettings(settings);
        const map: Record<string, FanProfile> = {};
        settings.fans.forEach((fan) => {
          map[fan.fanId] = fan;
        });
        setFanDrafts(map);
        setFanDraftsLoaded(true);
    } catch (err) {
      if (cancelled) return;
      setFanSaveStatus(`Fan settings load failed: ${(err as Error).message}`);
      setFanDraftsLoaded(true);
    }
  }

    loadFanSettings();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [s, h] = await Promise.all([
          getSummary(),
          getHistory(minutes),
        ]);
        if (cancelled) return;
        setSummary(s);
        setHistory(h);
        setError(null);
        setLoading(false);
      } catch (err) {
        if (cancelled) return;
        setError((err as Error).message);
        setLoading(false);
      }
    }

    load();
    const interval = setInterval(load, refreshMs);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [minutes, refreshMs]);

  useEffect(() => {
    if (!summary || !fanDraftsLoaded) return;
    const maxRecorded = fanSettings?.maxRecordedRpm ?? {};
    setFanDrafts((prev) => {
      let changed = false;
      const next = { ...prev };
      summary.fans.forEach((fan) => {
        if (!next[fan.label]) {
          const maxRpm = Math.round(
            maxRecorded[fan.label] ?? fan.rpm ?? 4000
          );
          next[fan.label] = buildDefaultFanProfile(fan.label, maxRpm);
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, [summary, fanSettings, fanDraftsLoaded, buildDefaultFanProfile]);

  const cpuTrend = useMemo(
    () =>
      history?.host
        .map((p) => p.cpuPercent ?? 0)
        .filter((v) => !Number.isNaN(v)) ?? [],
    [history]
  );
  const memTrend = useMemo(
    () =>
      history?.host
        .map((p) => p.memoryPercent ?? 0)
        .filter((v) => !Number.isNaN(v)) ?? [],
    [history]
  );

  const primaryGpu: GpuSample | undefined = summary?.gpus[0];
  const netTop: NetSample | undefined = summary?.networks[0];
  const gpuTempSummary =
    summary?.gpus.length
      ? summary.gpus
          .map((gpu) => `GPU ${gpu.gpuIndex}: ${fmtTemp(gpu.temp)}`)
          .join(" | ")
      : "No GPU telemetry";
  const fansWithRpm = useMemo(
    () => (summary?.fans ?? []).filter((fan) => (fan.rpm ?? 0) > 0),
    [summary]
  );
  const fansNoRpm = useMemo(
    () => (summary?.fans ?? []).filter((fan) => !fan.rpm || fan.rpm <= 0),
    [summary]
  );
  const maxFanRpm = useMemo(
    () =>
      fansWithRpm.length
        ? Math.max(...fansWithRpm.map((fan) => fan.rpm ?? 0))
        : 0,
    [fansWithRpm]
  );
  const gpuHistory = useMemo(() => {
    const map: Record<
      number,
      { temps: number[]; util: number[]; vramPct: number[] }
    > = {};
    history?.gpus.forEach((sample) => {
      const entry =
        map[sample.gpuIndex] ??
        (map[sample.gpuIndex] = { temps: [], util: [], vramPct: [] });
      if (typeof sample.temp === "number") {
        entry.temps.push(sample.temp);
      }
      if (typeof sample.util === "number") {
        entry.util.push(sample.util);
      }
      if (
        typeof sample.vramUsedMb === "number" &&
        typeof sample.vramTotalMb === "number" &&
        sample.vramTotalMb > 0
      ) {
        entry.vramPct.push((sample.vramUsedMb / sample.vramTotalMb) * 100);
      }
    });
    return map;
  }, [history]);

  const maxRecordedMap = fanSettings?.maxRecordedRpm ?? {};

  const fanDisplayMap = useMemo(() => {
    const map: Record<string, string> = {};
    Object.values(fanDrafts).forEach((fan) => {
      const name = fan.displayName?.trim();
      if (name) {
        map[fan.fanId] = name;
      }
    });
    return map;
  }, [fanDrafts]);

  const gpuOptions = useMemo(() => {
    const options = new Map<number, string>();
    (summary?.gpus ?? []).forEach((gpu) => {
      options.set(gpu.gpuIndex, gpu.label ?? `GPU ${gpu.gpuIndex}`);
    });
    return Array.from(options.entries()).map(([gpuIndex, label]) => ({
      gpuIndex,
      label,
    }));
  }, [summary]);

  const updateFanDraft = useCallback(
    (fanId: string, patch: Partial<FanProfile>) => {
      setFanDrafts((prev) => {
        const current = prev[fanId];
        if (!current) return prev;
        const next = { ...current, ...patch };
        return { ...prev, [fanId]: next };
      });
      setFanDirty(true);
    },
    []
  );

  const updateFanCurve = useCallback(
    (fanId: string, curve: FanCurvePoint[]) => {
      updateFanDraft(fanId, { curve });
    },
    [updateFanDraft]
  );

  const handleFanModeChange = useCallback(
    (fanId: string, mode: "default" | "override") => {
      setFanDrafts((prev) => {
        const current = prev[fanId];
        if (!current) return prev;
        const maxRpm =
          current.maxRpm ??
          Math.round(maxRecordedMap[fanId] ?? DefaultFanMaxRpm);
        const next = {
          ...current,
          mode,
          maxRpm,
          curve: current.curve?.length
            ? current.curve
            : defaultFanCurve(maxRpm),
        };
        return { ...prev, [fanId]: next };
      });
      setFanDirty(true);
    },
    [defaultFanCurve, maxRecordedMap, DefaultFanMaxRpm]
  );

  const handleCurveModeChange = useCallback(
    (fanId: string, curveMode: FanProfile["curveMode"]) => {
      setFanDrafts((prev) => {
        const current = prev[fanId];
        if (!current) return prev;
        const maxRpm = current.maxRpm ?? DefaultFanMaxRpm;
        const next = {
          ...current,
          curveMode,
          curve: current.curve?.length
            ? current.curve
            : defaultFanCurve(maxRpm),
          curveSource:
            curveMode === "custom"
              ? current.curveSource ?? "cpu"
              : curveMode,
          gpuIndices:
            curveMode === "gpu" && current.gpuIndices.length === 0
              ? gpuOptions.map((gpu) => gpu.gpuIndex)
              : current.gpuIndices,
        };
        return { ...prev, [fanId]: next };
      });
      setFanDirty(true);
    },
    [defaultFanCurve, gpuOptions, DefaultFanMaxRpm]
  );

  const handleCurveSourceChange = useCallback(
    (fanId: string, curveSource: "cpu" | "gpu") => {
      setFanDrafts((prev) => {
        const current = prev[fanId];
        if (!current) return prev;
        const next = {
          ...current,
          curveSource,
          gpuIndices:
            curveSource === "gpu" && current.gpuIndices.length === 0
              ? gpuOptions.map((gpu) => gpu.gpuIndex)
              : current.gpuIndices,
        };
        return { ...prev, [fanId]: next };
      });
      setFanDirty(true);
    },
    [gpuOptions]
  );

  const handleGpuToggle = useCallback(
    (fanId: string, gpuIndex: number) => {
      setFanDrafts((prev) => {
        const current = prev[fanId];
        if (!current) return prev;
        const exists = current.gpuIndices.includes(gpuIndex);
        const nextGpu = exists
          ? current.gpuIndices.filter((idx) => idx !== gpuIndex)
          : [...current.gpuIndices, gpuIndex];
        const next = { ...current, gpuIndices: nextGpu };
        return { ...prev, [fanId]: next };
      });
      setFanDirty(true);
    },
    []
  );

  const handleSaveFanSettings = useCallback(async () => {
    if (fanSaving) return;
    setFanSaving(true);
    setFanSaveStatus(null);
    try {
      const updated = await saveFanSettings(Object.values(fanDrafts));
      setFanSettings(updated);
      const map: Record<string, FanProfile> = {};
      updated.fans.forEach((fan) => {
        map[fan.fanId] = fan;
      });
      setFanDrafts(map);
      setFanDirty(false);
      setFanSaveStatus("Fan settings saved.");
    } catch (err) {
      setFanSaveStatus(`Save failed: ${(err as Error).message}`);
    } finally {
      setFanSaving(false);
    }
  }, [fanDrafts, fanSaving]);

  const handleReloadFanSettings = useCallback(async () => {
    if (fanSaving) return;
    setFanSaveStatus(null);
    try {
      const settings = await getFanSettings();
      setFanSettings(settings);
      const map: Record<string, FanProfile> = {};
      settings.fans.forEach((fan) => {
        map[fan.fanId] = fan;
      });
      setFanDrafts(map);
      setFanDraftsLoaded(true);
      setFanDirty(false);
      setFanSaveStatus("Fan settings reloaded.");
    } catch (err) {
      setFanSaveStatus(`Reload failed: ${(err as Error).message}`);
    }
  }, [fanSaving]);

  const normalizeFan = useCallback((fan: FanProfile) => {
    const sortedCurve = [...(fan.curve ?? [])].sort(
      (a, b) => a.tempC - b.tempC
    );
    const sortedGpu = [...(fan.gpuIndices ?? [])].sort((a, b) => a - b);
    return {
      ...fan,
      displayName: fan.displayName?.trim() ?? "",
      curve: sortedCurve,
      gpuIndices: sortedGpu,
    };
  }, []);

  const computeDirty = useCallback(
    (drafts: Record<string, FanProfile>, settings: FanSettingsResponse | null) => {
      if (!settings) return true;
      const baseMap: Record<string, FanProfile> = {};
      settings.fans.forEach((fan) => {
        baseMap[fan.fanId] = fan;
      });
      const draftKeys = Object.keys(drafts).sort();
      const baseKeys = Object.keys(baseMap).sort();
      if (draftKeys.join("|") !== baseKeys.join("|")) return true;
      return draftKeys.some((key) => {
        const draft = normalizeFan(drafts[key]);
        const base = normalizeFan(baseMap[key]);
        return JSON.stringify(draft) !== JSON.stringify(base);
      });
    },
    [normalizeFan]
  );

  const handleApplyFanSettings = useCallback(
    async (fanId: string) => {
      if (fanSaving) return;
      const fan = fanDrafts[fanId];
      if (!fan) return;
      setFanSaving(true);
      setFanSaveStatus(null);
      try {
        const updated = await saveFanSettings([fan]);
        setFanSettings(updated);
        const map: Record<string, FanProfile> = {};
        updated.fans.forEach((entry) => {
          map[entry.fanId] = entry;
        });
        const nextDrafts = { ...fanDrafts, [fanId]: map[fanId] ?? fan };
        setFanDrafts(nextDrafts);
        setFanDirty(computeDirty(nextDrafts, updated));
        setFanSaveStatus(`Applied settings for ${fanId}.`);
      } catch (err) {
        setFanSaveStatus(`Apply failed: ${(err as Error).message}`);
      } finally {
        setFanSaving(false);
      }
    },
    [fanDrafts, fanSaving, computeDirty]
  );

  const formatFanLabel = useCallback(
    (fanId: string) => {
      const name = fanDisplayMap[fanId];
      return name ? `${name} (${fanId})` : fanId;
    },
    [fanDisplayMap]
  );

  const hasOverrides = useMemo(
    () => Object.values(fanDrafts).some((fan) => fan.mode === "override"),
    [fanDrafts]
  );

  useEffect(() => {
    if (!fanSettingsOpenId) return;
    if (fanDrafts[fanSettingsOpenId]?.mode !== "override") {
      setFanSettingsOpenId(null);
    }
  }, [fanDrafts, fanSettingsOpenId]);

  const updated = summary
    ? new Date(summary.timestamp).toLocaleTimeString()
    : "—";

  const themeClass = theme === "night" ? "theme-night" : "theme-phosphor";

  return (
    <div className={`app-shell ${themeClass}`}>
      <header className="nav">
        <div className="nav-top">
          <div className="brand">
            <span className="brand-badge">S</span>
            <div>
              <div>sentra</div>
              <div className="muted" style={{ fontSize: 12 }}>
                observability node
              </div>
            </div>
          </div>
          <div className="cta-row">
            <span className="pill">updated {updated}</span>
            <button
              className="ghost-btn"
              onClick={() =>
                setTheme((prev) => (prev === "night" ? "phosphor" : "night"))
              }
            >
              {theme === "night" ? "Phosphor mode" : "Night mode"}
            </button>
            <select
              className="ghost-btn"
              value={refreshMs}
              onChange={(e) => setRefreshMs(Number(e.target.value))}
            >
              <option value={3000}>3s</option>
              <option value={5000}>5s</option>
              <option value={10000}>10s</option>
            </select>
          </div>
        </div>
        {summary ? (
          <div className="nav-stats">
            <div className="nav-stat">
              <div className="nav-stat-label">CPU</div>
              <div className="nav-stat-value">{fmtPct(summary.cpu.totalUtil)}</div>
              <Sparkline
                points={cpuTrend}
                color="var(--phosphor)"
                height={34}
                strokeWidth={1}
              />
            </div>
            <div className="nav-stat">
              <div className="nav-stat-label">Memory</div>
              <div className="nav-stat-value">
                {fmtPct(summary.memory.usedPercent)}
              </div>
              <Sparkline
                points={memTrend}
                color="#7ef2b7"
                height={34}
                strokeWidth={1}
              />
              <div className="nav-stat-hint">
                {fmtBytes(summary.memory.usedBytes)} / {fmtBytes(summary.memory.totalBytes)}
              </div>
            </div>
            <div className="nav-stat">
              <div className="nav-stat-label">CPU Temp</div>
              <div className="nav-stat-value">{fmtTemp(summary.cpu.temp)}</div>
              <div className="nav-stat-hint">
                uptime {fmtDuration(summary.cpu.uptimeSec)}
              </div>
            </div>
            <div className="nav-stat nav-stat--wide">
              <div className="nav-stat-label">GPU Temps</div>
              <div className="nav-stat-value nav-stat-value--compact">{gpuTempSummary}</div>
            </div>
          </div>
        ) : null}
      </header>

      <section className="hero">
        <div>
          <h1>Real-time insight without the Streamlit glass.</h1>
          <p>
            This new React dashboard talks to a lightweight C# API that reads
            the same MySQL data the collectors fill. Swap between time windows,
            watch GPU thermals, and see how disks and network are behaving
            without opening a notebook.
          </p>
          <div className="cta-row" style={{ marginTop: 14 }}>
            <span className="pill">
              <span>API</span>
              <span className="badge">/api/telemetry</span>
            </span>
            <span className="pill window-pill">
              <span>Window</span>
              <div className="slider-wrap">
                <input
                  type="range"
                  min={15}
                  max={480}
                  step={15}
                  value={minutes}
                  onChange={(e) => setMinutes(Number(e.target.value))}
                />
                <div className="slider-value">
                  {minutes >= 60
                    ? `${(minutes / 60).toFixed(minutes % 60 ? 1 : 0)}h`
                    : `${minutes}m`}
                </div>
              </div>
            </span>
          </div>
        </div>
        <div className="hero-metrics">
          <StatCard
            title="CPU"
            value={fmtPct(summary?.cpu.totalUtil)}
            hint={`load: ${fmtPct(summary?.cpu.load1)} / sys: ${fmtPct(
              summary?.cpu.systemPct
            )} / user: ${fmtPct(summary?.cpu.userPct)}`}
            accent="var(--phosphor)"
          >
            <Bar pct={summary?.cpu.totalUtil} />
            <Sparkline
              points={cpuTrend}
              color="var(--phosphor)"
              height={48}
              strokeWidth={1.1}
            />
          </StatCard>
          <StatCard
            title="Memory"
            value={fmtPct(summary?.memory.usedPercent)}
            hint={`${fmtBytes(summary?.memory.usedBytes)} of ${fmtBytes(
              summary?.memory.totalBytes
            )}`}
            accent="var(--phosphor)"
          >
            <Bar pct={summary?.memory.usedPercent} />
            <Sparkline
              points={memTrend}
              color="#7ef2b7"
              height={48}
              strokeWidth={1.1}
            />
          </StatCard>
          <StatCard
            title="Primary GPU"
            value={
              primaryGpu
                ? `${fmtTemp(primaryGpu.temp)} - ${fmtPct(primaryGpu.util)}`
                : "N/A"
            }
            hint={
              primaryGpu
                ? `VRAM ${primaryGpu.vramUsedMb ?? 0} / ${
                    primaryGpu.vramTotalMb ?? "?"
                  } MB`
                : "No GPU samples yet"
            }
          >
            <Bar pct={primaryGpu?.util} />
          </StatCard>
        </div>
      </section>

      {error && <div className="error">API error: {error}</div>}
      {loading && <div className="loading">Loading telemetry…</div>}

      {summary && (
        <>
          <div className="card-grid">
            <StatCard
              title="CPU Temp"
              value={fmtTemp(summary.cpu.temp)}
              hint={`uptime ${fmtDuration(summary.cpu.uptimeSec)}`}
            />
            <StatCard
              title="Swap"
              value={fmtPct(summary.memory.swapUsedPercent)}
              hint="swap utilization"
            >
              <Bar pct={summary.memory.swapUsedPercent} />
            </StatCard>
            <StatCard
              title="Disks"
              value={
                summary.disks.length
                  ? `${summary.disks.length} devices`
                  : "No data"
              }
              hint={
                summary.disks.length
                  ? `avg usage ${avg(
                      summary.disks.map((d) => d.usagePercent ?? 0)
                    ).toFixed(1)}%`
                  : "waiting for samples"
              }
            />
            <StatCard
              title="Network"
              value={
                netTop
                  ? `${fmtBps(netTop.rxBps)} / ${fmtBps(netTop.txBps)}`
                  : "N/A"
              }
              hint={netTop ? netTop.interface : "no interfaces yet"}
            />
          </div>

          <section className="section">
            <div className="section-header">
              <div className="section-title">
                Trends (last {minutes} minutes)
              </div>
              <div className="cta-row">
                <span className="pill">
                  host samples: {history?.host.length ?? 0}
                </span>
                <span className="pill">
                  gpu samples: {history?.gpus.length ?? 0}
                </span>
              </div>
            </div>
            <div className="grid-2">
              <div className="card">
                <h3>CPU %</h3>
                <Sparkline
                  points={cpuTrend}
                  color="var(--phosphor)"
                  height={32}
                  strokeWidth={0.9}
                />
              </div>
              <div className="card">
                <h3>Memory %</h3>
                <Sparkline
                  points={memTrend}
                  color="#7ef2b7"
                  height={32}
                  strokeWidth={0.9}
                />
              </div>
            </div>
          </section>

          <section className="section">
            <div className="section-header">
              <div className="section-title">GPU Health</div>
              <span className="pill">
                {summary.gpus.length
                  ? `${summary.gpus.length} devices`
                  : "waiting for samples"}
              </span>
            </div>
            <div className="grid-2 gpu-grid">
              {summary.gpus.map((gpu) => {
                const historyEntry = gpuHistory[gpu.gpuIndex] ?? {
                  temps: [],
                  util: [],
                  vramPct: [],
                };
                const vramPct =
                  gpu.vramTotalMb && gpu.vramTotalMb > 0 && gpu.vramUsedMb
                    ? (gpu.vramUsedMb / gpu.vramTotalMb) * 100
                    : undefined;
                return (
                  <div key={gpu.gpuIndex} className="card gpu-card">
                    <div className="gpu-card-head">
                      <h3>GPU {gpu.gpuIndex}</h3>
                      <div className="gpu-stats-primary">
                        <span>{fmtTemp(gpu.temp)}</span>
                        <span>-</span>
                        <span>{fmtPct(gpu.util)}</span>
                      </div>
                    </div>
                    <div className="gpu-sparklines">
                      <MiniTrend
                        label="Temp"
                        points={historyEntry.temps}
                        color="var(--phosphor)"
                        format={(v) => fmtTemp(v)}
                      />
                      <MiniTrend
                        label="Usage"
                        points={historyEntry.util}
                        color="#7ef2b7"
                        format={(v) => fmtPct(v)}
                      />
                    </div>
                    <p className="hint">
                      VRAM {gpu.vramUsedMb ?? 0} / {gpu.vramTotalMb ?? "?"} MB
                    </p>
                    <Bar pct={vramPct} />
                  </div>
                );
              })}
              {!summary.gpus.length && (
                <div className="muted">No GPU rows in DB yet.</div>
              )}
            </div>
          </section>

          
          <section className="section">
            <div className="section-header">
              <div className="section-title">Fans</div>
              <div className="cta-row">
                {fanDirty && <span className="pill">unsaved changes</span>}
                <button
                  className="ghost-btn"
                  disabled={!fanDraftsLoaded || fanSaving}
                  onClick={handleSaveFanSettings}
                >
                  {fanSaving ? "Saving..." : "Save settings"}
                </button>
                <button
                  className="ghost-btn"
                  disabled={fanSaving}
                  onClick={handleReloadFanSettings}
                >
                  Reload
                </button>
              </div>
            </div>
            <div className="fan-gauges">
              {fansWithRpm.length ? (
                fansWithRpm.map((fan) => (
                  <FanGauge
                    key={fan.label}
                    fan={fan}
                    maxRpm={
                      fanDrafts[fan.label]?.maxRpm ??
                      Math.round(maxRecordedMap[fan.label] ?? maxFanRpm ?? 1)
                    }
                    displayName={fanDisplayMap[fan.label]}
                  />
                ))
              ) : (
                <div className="muted">no fans reporting RPM yet</div>
              )}
            </div>
            {fansNoRpm.length > 0 && (
              <div className="fan-missing">
                <div className="fan-missing-title">No RPM reading detected</div>
                <div className="chip-row">
                  {fansNoRpm.map((fan) => (
                    <span className="chip" key={`missing-${fan.label}`}>
                      {formatFanLabel(fan.label)}
                    </span>
                  ))}
                </div>
              </div>
            )}

            <div className="fan-control-stack">
              <div className="fan-control-header">
                <div>
                  <h3>Fan Control Modes</h3>
                  <p className="hint">
                    Default mode reads RPM only. Override mode writes your fan
                    targets directly, so use it at your own risk.
                  </p>
                </div>
                {fanSaveStatus && <span className="badge">{fanSaveStatus}</span>}
              </div>

              {hasOverrides && (
                <div className="fan-warning">
                  Override mode can damage hardware if misused. Confirm your
                  curves before applying.
                </div>
              )}

              <div className="fan-control-grid">
                {Object.values(fanDrafts)
                  .sort((a, b) => a.fanId.localeCompare(b.fanId))
                  .filter((fan) => {
                    const liveFan = summary?.fans.find(
                      (entry) => entry.label === fan.fanId
                    );
                    const maxRecorded = maxRecordedMap[fan.fanId] ?? 0;
                    const liveRpm = liveFan?.rpm ?? 0;
                    return maxRecorded > 0 || liveRpm > 0;
                  })
                  .map((fan) => {
                    const liveFan = summary?.fans.find(
                      (entry) => entry.label === fan.fanId
                    );
                    const maxRecorded =
                      maxRecordedMap[fan.fanId] ?? liveFan?.rpm ?? 0;
                    const maxRpm = fan.maxRpm ?? Math.round(maxRecorded || 4000);
                    return (
                      <div key={fan.fanId} className="card fan-control-card">
                        <div className="fan-control-head">
                          <div>
                            <div className="fan-control-title">
                              {fan.displayName?.trim() || fan.fanId}
                            </div>
                            <div className="fan-control-subtitle">
                              ID {fan.fanId}
                            </div>
                          </div>
                          <div className="fan-control-badges">
                            <span className="badge">
                              live {liveFan?.rpm?.toFixed(0) ?? 0} RPM
                            </span>
                            <span className="badge">
                              max seen {Math.round(maxRecorded)} RPM
                            </span>
                            {fan.mode === "override" && (
                              <button
                                className="ghost-btn ghost-btn--small"
                                onClick={() => setFanSettingsOpenId(fan.fanId)}
                              >
                                Settings
                              </button>
                            )}
                          </div>
                        </div>

                        <div className="fan-control-row">
                          <label>Custom name</label>
                          <input
                            className="ghost-input"
                            type="text"
                            placeholder="Optional display name"
                            value={fan.displayName ?? ""}
                            onChange={(e) =>
                              updateFanDraft(fan.fanId, {
                                displayName: e.target.value,
                              })
                            }
                          />
                        </div>

                        <div className="fan-control-row">
                          <label>Mode</label>
                          <div className="segmented">
                            <button
                              className={
                                fan.mode === "default"
                                  ? "segmented-btn active"
                                  : "segmented-btn"
                              }
                              onClick={() =>
                                handleFanModeChange(fan.fanId, "default")
                              }
                            >
                              Default
                            </button>
                            <button
                              className={
                                fan.mode === "override"
                                  ? "segmented-btn active"
                                  : "segmented-btn"
                              }
                              onClick={() =>
                                handleFanModeChange(fan.fanId, "override")
                              }
                            >
                              Override
                            </button>
                          </div>
                        </div>
                      </div>
                    );
                  })}
              </div>

              {fanSettingsOpenId && fanDrafts[fanSettingsOpenId] && (
                <div className="modal-backdrop">
                  <div className="modal">
                    <div className="modal-header">
                      <div>
                        <div className="modal-title">
                          Override settings
                        </div>
                        <div className="modal-subtitle">
                          {fanDrafts[fanSettingsOpenId].displayName?.trim() ||
                            fanSettingsOpenId}
                        </div>
                      </div>
                      <button
                        className="ghost-btn ghost-btn--small"
                        onClick={() => setFanSettingsOpenId(null)}
                      >
                        Close
                      </button>
                    </div>

                    <div className="fan-modal-body">
                      <div className="fan-control-row">
                        <label>Max RPM</label>
                        <input
                          className="ghost-input"
                          type="number"
                          min={0}
                          step={50}
                          value={
                            fanDrafts[fanSettingsOpenId].maxRpm ??
                            Math.round(
                              maxRecordedMap[fanSettingsOpenId] ?? 4000
                            )
                          }
                          onChange={(e) => {
                            const nextMax = Number(e.target.value);
                            const fan = fanDrafts[fanSettingsOpenId];
                            const currentMax =
                              fan.maxRpm ??
                              Math.round(
                                maxRecordedMap[fanSettingsOpenId] ?? 4000
                              );
                            const scaledCurve = fan.curve.length
                              ? fan.curve.map((point) => ({
                                  ...point,
                                  rpm: Math.round(
                                    (point.rpm / Math.max(currentMax, 1)) *
                                      Math.max(nextMax, 1)
                                  ),
                                }))
                              : defaultFanCurve(nextMax);
                            updateFanDraft(fanSettingsOpenId, {
                              maxRpm: nextMax,
                              curve: scaledCurve,
                            });
                          }}
                        />
                        <div className="fan-control-hint">
                          default from{" "}
                          {Math.round(maxRecordedMap[fanSettingsOpenId] ?? 0)} RPM,
                          can exceed
                        </div>
                      </div>

                      <div className="fan-control-row">
                        <label>Curve mode</label>
                        <select
                          className="ghost-input"
                          value={fanDrafts[fanSettingsOpenId].curveMode}
                          onChange={(e) =>
                            handleCurveModeChange(
                              fanSettingsOpenId,
                              e.target.value as FanProfile["curveMode"]
                            )
                          }
                        >
                          <option value="custom">User curve</option>
                          <option value="cpu">CPU temp curve</option>
                          <option value="gpu">GPU temp curve</option>
                        </select>
                      </div>

                      {fanDrafts[fanSettingsOpenId].curveMode === "custom" && (
                        <div className="fan-control-row">
                          <label>Temp source</label>
                          <select
                            className="ghost-input"
                            value={fanDrafts[fanSettingsOpenId].curveSource ?? "cpu"}
                            onChange={(e) =>
                              handleCurveSourceChange(
                                fanSettingsOpenId,
                                e.target.value as "cpu" | "gpu"
                              )
                            }
                          >
                            <option value="cpu">CPU sensors</option>
                            <option value="gpu">GPU sensors</option>
                          </select>
                        </div>
                      )}

                      {(fanDrafts[fanSettingsOpenId].curveMode === "gpu" ||
                        (fanDrafts[fanSettingsOpenId].curveMode === "custom" &&
                          fanDrafts[fanSettingsOpenId].curveSource === "gpu")) && (
                        <div className="fan-control-row fan-gpu-row">
                          <label>GPU pool</label>
                          {gpuOptions.length ? (
                            <div className="chip-row">
                              {gpuOptions.map((gpu) => (
                                <button
                                  key={gpu.gpuIndex}
                                  className={
                                    fanDrafts[fanSettingsOpenId].gpuIndices.includes(
                                      gpu.gpuIndex
                                    )
                                      ? "chip chip-active"
                                      : "chip"
                                  }
                                  onClick={() =>
                                    handleGpuToggle(fanSettingsOpenId, gpu.gpuIndex)
                                  }
                                >
                                  {gpu.label}
                                </button>
                              ))}
                            </div>
                          ) : (
                            <div className="muted">no GPUs detected for pooling</div>
                          )}
                          <div className="fan-control-hint">
                            Uses max temperature of selected GPUs.
                          </div>
                        </div>
                      )}

                      <FanCurveEditor
                        curve={fanDrafts[fanSettingsOpenId].curve}
                        maxRpm={
                          fanDrafts[fanSettingsOpenId].maxRpm ??
                          Math.round(maxRecordedMap[fanSettingsOpenId] ?? 4000)
                        }
                        onChange={(nextCurve) =>
                          updateFanCurve(fanSettingsOpenId, nextCurve)
                        }
                      />
                    </div>

                    <div className="modal-footer">
                      <button
                        className="ghost-btn"
                        onClick={() => handleApplyFanSettings(fanSettingsOpenId)}
                        disabled={fanSaving}
                      >
                        {fanSaving ? "Applying..." : "Apply"}
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </section>

          <section className="section">
            <div className="section-header">
              <div className="section-title">Disks</div>
            </div>
            <div className="grid-2">
              <div className="card">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Device</th>
                      <th>Usage</th>
                      <th>Read</th>
                      <th>Write</th>
                    </tr>
                  </thead>
                  <tbody>
                    {summary.disks.map((d) => (
                      <tr key={d.device}>
                        <td>{d.device}</td>
                        <td>{fmtPct(d.usagePercent)}</td>
                        <td>{fmtBps(d.readBps)}</td>
                        <td>{fmtBps(d.writeBps)}</td>
                      </tr>
                    ))}
                    {!summary.disks.length && (
                      <tr>
                        <td colSpan={4} className="muted">
                          waiting for disk samples
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              <div className="card">
                <h3>Network</h3>
                <div className="list">
                  {summary.networks.map((n) => (
                    <div key={n.interface} className="net-row">
                      <div>
                        <div>{n.interface}</div>
                        <div className="muted">rx / tx</div>
                      </div>
                      <div className="badge">{fmtBps(n.rxBps)}</div>
                      <div className="badge">{fmtBps(n.txBps)}</div>
                    </div>
                  ))}
                  {!summary.networks.length && (
                    <div className="muted">waiting for network samples</div>
                  )}
                </div>
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  );
}

type FanCurveEditorProps = {
  curve: FanCurvePoint[];
  maxRpm: number;
  onChange: (curve: FanCurvePoint[]) => void;
};

function FanCurveEditor({ curve, maxRpm, onChange }: FanCurveEditorProps) {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const minTemp = 20;
  const maxTemp = 100;
  const minPct = 0;
  const maxPct = 100;
  const chartWidth = 320;
  const chartHeight = 180;
  const pad = 24;
  const points = useMemo(() => {
    const base = curve.length ? curve : [{ tempC: 60, rpm: maxRpm }];
    return [...base].sort((a, b) => a.tempC - b.tempC);
  }, [curve, maxRpm]);

  const clamp = (value: number, min: number, max: number) =>
    Math.max(min, Math.min(max, value));

  const rpmToPct = useCallback(
    (rpm: number) => clamp(Math.round((rpm / Math.max(maxRpm, 1)) * 100), 0, 100),
    [maxRpm]
  );

  const pctToRpm = useCallback(
    (pct: number) => Math.round((clamp(pct, 0, 100) / 100) * Math.max(maxRpm, 1)),
    [maxRpm]
  );

  const toCoord = useCallback(
    (point: FanCurvePoint) => {
      const x =
        pad +
        ((point.tempC - minTemp) / (maxTemp - minTemp)) *
          (chartWidth - pad * 2);
      const pct = rpmToPct(point.rpm);
      const y =
        pad +
        (1 - (pct - minPct) / (maxPct - minPct)) *
          (chartHeight - pad * 2);
      return { x, y, pct };
    },
    [chartHeight, chartWidth, minTemp, maxTemp, minPct, maxPct, pad, rpmToPct]
  );

  const handlePointChange = (
    index: number,
    field: "tempC" | "rpm",
    value: number
  ) => {
    const next = points.map((point, idx) =>
      idx === index ? { ...point, [field]: value } : point
    );
    onChange(next.sort((a, b) => a.tempC - b.tempC));
  };

  const handleRemove = (index: number) => {
    if (points.length <= 2) return;
    const next = points.filter((_, idx) => idx !== index);
    onChange(next);
  };

  const handleAdd = () => {
    const last = points[points.length - 1];
    const nextTemp = Math.min(100, (last?.tempC ?? 70) + 5);
    const nextRpm = Math.min(maxRpm, Math.round((last?.rpm ?? maxRpm) * 1.05));
    onChange([...points, { tempC: nextTemp, rpm: nextRpm }]);
  };

  useEffect(() => {
    if (dragIndex === null) return;

    const handleMove = (event: MouseEvent) => {
      const svg = svgRef.current;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const innerWidth = chartWidth - pad * 2;
      const innerHeight = chartHeight - pad * 2;

      const clampedX = clamp(x, pad, pad + innerWidth);
      const clampedY = clamp(y, pad, pad + innerHeight);

      const temp =
        minTemp + ((clampedX - pad) / innerWidth) * (maxTemp - minTemp);
      const pct =
        maxPct -
        ((clampedY - pad) / innerHeight) * (maxPct - minPct);

      const prev = points[dragIndex - 1];
      const next = points[dragIndex + 1];
      const minTempBound = prev ? prev.tempC + 1 : minTemp;
      const maxTempBound = next ? next.tempC - 1 : maxTemp;
      const nextTemp = clamp(Math.round(temp), minTempBound, maxTempBound);
      const nextPct = clamp(Math.round(pct), minPct, maxPct);
      const nextRpm = pctToRpm(nextPct);

      const updated = points.map((point, idx) =>
        idx === dragIndex
          ? { ...point, tempC: nextTemp, rpm: nextRpm }
          : point
      );
      onChange(updated);
    };

    const handleUp = () => {
      setDragIndex(null);
    };

    window.addEventListener("mousemove", handleMove);
    window.addEventListener("mouseup", handleUp);
    return () => {
      window.removeEventListener("mousemove", handleMove);
      window.removeEventListener("mouseup", handleUp);
    };
  }, [
    chartHeight,
    chartWidth,
    dragIndex,
    maxPct,
    maxTemp,
    minPct,
    minTemp,
    onChange,
    pad,
    pctToRpm,
    points,
  ]);

  const path = points
    .map((point, idx) => {
      const { x, y } = toCoord(point);
      return `${idx === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");

  const fillPath = (() => {
    if (!points.length) return "";
    const start = toCoord(points[0]);
    const end = toCoord(points[points.length - 1]);
    return `${path} L ${end.x} ${chartHeight - pad} L ${start.x} ${
      chartHeight - pad
    } Z`;
  })();

  const gridTemps = [20, 40, 60, 80, 100];
  const gridPcts = [0, 25, 50, 75, 100];

  return (
    <div className="fan-curve-editor">
      <div className="fan-curve-head">
        <div>
          <div className="fan-curve-title">Curve points</div>
          <div className="fan-curve-hint">
            Drag the nodes or edit values to match your thermal targets.
          </div>
        </div>
        <button className="ghost-btn" onClick={handleAdd}>
          Add point
        </button>
      </div>
      <div className="fan-curve-chart">
        <svg
          ref={svgRef}
          width="100%"
          height={chartHeight}
          viewBox={`0 0 ${chartWidth} ${chartHeight}`}
          preserveAspectRatio="none"
        >
          <defs>
            <linearGradient id="fanCurveFill" x1="0" y1="0" x2="0" y2="1">
              <stop
                offset="0%"
                stopColor="rgba(var(--phosphor-rgb), 0.35)"
              />
              <stop
                offset="100%"
                stopColor="rgba(var(--phosphor-rgb), 0.05)"
              />
            </linearGradient>
          </defs>
          <rect
            x={pad}
            y={pad}
            width={chartWidth - pad * 2}
            height={chartHeight - pad * 2}
            className="fan-curve-bg"
          />
          {gridTemps.map((temp) => {
            const x =
              pad +
              ((temp - minTemp) / (maxTemp - minTemp)) *
                (chartWidth - pad * 2);
            return (
              <g key={`temp-${temp}`}>
                <line
                  x1={x}
                  y1={pad}
                  x2={x}
                  y2={chartHeight - pad}
                  className="fan-curve-grid"
                />
                <text x={x} y={chartHeight - 6} className="fan-curve-axis">
                  {temp}C
                </text>
              </g>
            );
          })}
          {gridPcts.map((pct) => {
            const y =
              pad +
              (1 - (pct - minPct) / (maxPct - minPct)) *
                (chartHeight - pad * 2);
            return (
              <g key={`pct-${pct}`}>
                <line
                  x1={pad}
                  y1={y}
                  x2={chartWidth - pad}
                  y2={y}
                  className="fan-curve-grid"
                />
                <text x={6} y={y + 4} className="fan-curve-axis fan-curve-axis--left">
                  {pct}%
                </text>
              </g>
            );
          })}
          <path d={fillPath} fill="url(#fanCurveFill)" />
          <path d={path} className="fan-curve-line" />
          {points.map((point, idx) => {
            const { x, y, pct } = toCoord(point);
            return (
              <g
                key={`point-${idx}`}
                className="fan-curve-point"
                onMouseDown={() => setDragIndex(idx)}
              >
                <circle cx={x} cy={y} r={7} />
                <text x={x} y={y - 12} className="fan-curve-label">
                  {pct}%
                </text>
              </g>
            );
          })}
        </svg>
        <div className="fan-curve-foot">
          <span>Temp (C)</span>
          <span>Fan %</span>
        </div>
      </div>
      <div className="fan-curve-table">
        <div className="fan-curve-row fan-curve-row--head">
          <span>Step</span>
          <span>Temp (C)</span>
          <span>Fan %</span>
          <span>RPM</span>
          <span>Action</span>
        </div>
        {points.map((point, idx) => {
          const pct = rpmToPct(point.rpm);
          return (
            <div key={`curve-${idx}`} className="fan-curve-row">
              <span className="muted">#{idx + 1}</span>
              <input
                className="ghost-input"
                type="number"
                min={minTemp}
                max={maxTemp}
                step={1}
                value={point.tempC}
                onChange={(e) =>
                  handlePointChange(idx, "tempC", Number(e.target.value))
                }
              />
              <input
                className="ghost-input"
                type="number"
                min={0}
                max={100}
                step={1}
                value={pct}
                onChange={(e) =>
                  handlePointChange(idx, "rpm", pctToRpm(Number(e.target.value)))
                }
              />
              <span className="fan-curve-rpm">{point.rpm} RPM</span>
              <button
                className="ghost-btn ghost-btn--quiet"
                onClick={() => handleRemove(idx)}
                disabled={points.length <= 2}
              >
                Remove
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function Bar({ pct }: { pct?: number }) {
  const clamped =
    pct === undefined || pct === null
      ? 0
      : Math.max(0, Math.min(100, Number(pct)));

  return (
    <div className="bar">
      <div className="bar-fill" style={{ width: `${clamped}%` }} />
    </div>
  );
}

function avg(values: number[]): number {
  if (!values.length) return 0;
  const sum = values.reduce((a, b) => a + b, 0);
  return sum / values.length;
}
