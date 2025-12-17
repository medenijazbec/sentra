import { FanSample } from "../types/telemetry";

type Props = {
  fan: FanSample;
  maxRpm: number;
};

export function FanGauge({ fan, maxRpm }: Props) {
  const rpm = Math.max(fan.rpm ?? 0, 0);
  const safeMax = Math.max(maxRpm, rpm, 1);
  const pct = Math.min(rpm / safeMax, 1);
  const radius = 50;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - pct * circumference;

  return (
    <div className="radial-gauge">
      <svg
        className="radial-gauge-svg"
        width="120"
        height="120"
        viewBox="0 0 120 120"
      >
        <circle className="gauge-track" cx="60" cy="60" r={radius} />
        <circle
          className="gauge-progress"
          cx="60"
          cy="60"
          r={radius}
          transform="rotate(-90 60 60)"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
        />
        <text x="60" y="60" className="gauge-value">
          {rpm.toFixed(0)}
        </text>
        <text x="60" y="78" className="gauge-unit">
          RPM
        </text>
      </svg>
      <div className="radial-label">{fan.label}</div>
    </div>
  );
}
