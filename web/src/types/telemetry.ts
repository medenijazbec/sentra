export type CpuSummary = {
  timestamp: string;
  totalUtil?: number;
  temp?: number;
  load1?: number;
  load5?: number;
  load15?: number;
  perCore: number[];
  uptimeSec?: number;
  userPct?: number;
  systemPct?: number;
};

export type MemorySummary = {
  timestamp: string;
  usedPercent?: number;
  usedBytes?: number;
  totalBytes?: number;
  swapUsedPercent?: number;
};

export type GpuSample = {
  timestamp: string;
  gpuIndex: number;
  temp?: number;
  util?: number;
  powerW?: number;
  vramUsedMb?: number;
  vramTotalMb?: number;
  fanPercent?: number;
};

export type DiskSample = {
  timestamp: string;
  device: string;
  readBps?: number;
  writeBps?: number;
  usagePercent?: number;
};

export type NetSample = {
  timestamp: string;
  interface: string;
  rxBps?: number;
  txBps?: number;
};

export type FanSample = {
  timestamp: string;
  label: string;
  rpm?: number;
};

export type TelemetrySummary = {
  timestamp: string;
  cpu: CpuSummary;
  memory: MemorySummary;
  gpus: GpuSample[];
  disks: DiskSample[];
  networks: NetSample[];
  fans: FanSample[];
};

export type HostHistoryPoint = {
  timestamp: string;
  cpuPercent?: number;
  cpuTemp?: number;
  memoryPercent?: number;
  swapPercent?: number;
};

export type HistoryResponse = {
  host: HostHistoryPoint[];
  gpus: GpuSample[];
};
