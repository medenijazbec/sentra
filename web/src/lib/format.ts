export function fmtPct(value?: number, digits = 1): string {
  if (value === undefined || value === null || Number.isNaN(value)) return "N/A";
  return `${value.toFixed(digits)}%`;
}

export function fmtBytes(value?: number): string {
  if (value === undefined || value === null) return "N/A";
  const kb = 1024;
  const mb = kb * 1024;
  const gb = mb * 1024;
  if (value >= gb) return `${(value / gb).toFixed(1)} GB`;
  if (value >= mb) return `${(value / mb).toFixed(1)} MB`;
  if (value >= kb) return `${(value / kb).toFixed(0)} KB`;
  return `${value} B`;
}

export function fmtBps(value?: number): string {
  if (value === undefined || value === null) return "N/A";
  const mb = 1_000_000;
  const kb = 1_000;
  if (value >= mb) return `${(value / mb).toFixed(1)} MB/s`;
  if (value >= kb) return `${(value / kb).toFixed(1)} KB/s`;
  return `${value.toFixed(0)} B/s`;
}

export function fmtTemp(value?: number): string {
  if (value === undefined || value === null) return "N/A";
  return `${value.toFixed(1)} C`;
}

export function fmtDuration(seconds?: number): string {
  if (!seconds || seconds <= 0) return "—";
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  if (hrs > 0) return `${hrs}h ${mins}m`;
  return `${mins}m`;
}
