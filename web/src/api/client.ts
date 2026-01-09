import {
  FanSettingsResponse,
  HistoryResponse,
  TelemetrySummary,
} from "../types/telemetry";

function resolveApiBase(): string {
  const fromEnv = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim();
  if (fromEnv && !fromEnv.includes("localhost:8080")) return fromEnv.replace(/\/$/, "");

  // Default: same-origin relative `/api` so nginx proxy works and avoids ad-blocks.
  return "";
}

const API_BASE = resolveApiBase();

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const url = API_BASE ? `${API_BASE}${path}` : path; // relative when base is ""
  const res = await fetch(url, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed (${res.status}): ${text}`);
  }
  return (await res.json()) as T;
}

export async function getSummary(): Promise<TelemetrySummary> {
  return request<TelemetrySummary>("/api/telemetry/summary");
}

export async function getHistory(minutes: number): Promise<HistoryResponse> {
  const window = minutes > 0 ? minutes : 60;
  return request<HistoryResponse>(`/api/telemetry/history?minutes=${window}`);
}

export async function getFanSettings(): Promise<FanSettingsResponse> {
  return request<FanSettingsResponse>("/api/fans/settings");
}

export async function saveFanSettings(
  fans: FanSettingsResponse["fans"]
): Promise<FanSettingsResponse> {
  return request<FanSettingsResponse>("/api/fans/settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fans }),
  });
}
