import { useEffect, useMemo, useState } from "react";
import { getHistory, getSummary } from "./api/client";
import {
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
  const [summary, setSummary] = useState<TelemetrySummary | null>(null);
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [minutes, setMinutes] = useState(240);
  const [refreshMs, setRefreshMs] = useState(5000);
  const [loading, setLoading] = useState(true);
  const [theme, setTheme] = useState<"phosphor" | "night">("phosphor");

  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove("theme-night", "theme-phosphor");
    root.classList.add(theme === "night" ? "theme-night" : "theme-phosphor");
  }, [theme]);

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
            the same SQLite data the collectors fill. Swap between time windows,
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
            </div>
            <div className="fan-gauges">
              {fansWithRpm.length ? (
                fansWithRpm.map((fan) => (
                  <FanGauge key={fan.label} fan={fan} maxRpm={maxFanRpm || 1} />
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
                      {fan.label}
                    </span>
                  ))}
                </div>
              </div>
            )}
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
