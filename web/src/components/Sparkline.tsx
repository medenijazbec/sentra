type Props = {
  points: number[];
  color?: string;
  height?: number;
  strokeWidth?: number;
};

export function Sparkline({
  points,
  color = "var(--phosphor)",
  height = 56,
  strokeWidth = 1.4,
}: Props) {
  if (!points.length) {
    return <div className="sparkline muted">no data</div>;
  }

  const viewWidth = 100;
  const viewHeight = 40;
  const max = Math.max(...points, 1);
  const min = Math.min(...points, 0);
  const span = max - min || 1;

  const step = viewWidth / Math.max(points.length - 1, 1);
  const coords = points
    .map((p, idx) => {
      const x = idx * step;
      const norm = (p - min) / span;
      const y = viewHeight - norm * viewHeight;
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <svg
      className="sparkline"
      style={{ height }}
      viewBox={`0 0 ${viewWidth} ${viewHeight}`}
      preserveAspectRatio="none"
      shapeRendering="geometricPrecision"
    >
      <polyline
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        points={coords}
      />
    </svg>
  );
}
