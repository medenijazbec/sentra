import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type Props = {
  points: number[];
  label: string;
  color?: string;
  height?: number;
  format?: (value?: number) => string;
};

export function MiniTrend({
  points,
  label,
  color = "var(--phosphor)",
  height = 36,
  format = (v) => (v ?? 0).toFixed(1),
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [activeIdx, setActiveIdx] = useState(() =>
    points.length ? points.length - 1 : 0
  );

  useEffect(() => {
    setActiveIdx(points.length ? points.length - 1 : 0);
  }, [points]);

  const viewWidth = 100;
  const viewHeight = 40;
  const coords = useMemo(() => {
    if (!points.length) return "";
    const max = Math.max(...points, 1);
    const min = Math.min(...points, 0);
    const span = max - min || 1;
    const step = viewWidth / Math.max(points.length - 1, 1);
    return points
      .map((p, idx) => {
        const x = idx * step;
        const norm = (p - min) / span;
        const y = viewHeight - norm * viewHeight;
        return `${x},${y}`;
      })
      .join(" ");
  }, [points]);

  const moveToPosition = useCallback(
    (clientX: number) => {
      if (!containerRef.current || !points.length) return;
      const rect = containerRef.current.getBoundingClientRect();
      const ratio = Math.max(
        0,
        Math.min(1, (clientX - rect.left) / Math.max(rect.width, 1))
      );
      const idx = Math.round(ratio * Math.max(points.length - 1, 0));
      setActiveIdx(idx);
    },
    [points]
  );

  const onPointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
    moveToPosition(e.clientX);
  };

  const onPointerLeave = () => {
    setActiveIdx(points.length ? points.length - 1 : 0);
  };

  const active =
    points.length && activeIdx >= 0 && activeIdx < points.length
      ? points[activeIdx]
      : undefined;

  const cursorX =
    points.length > 1
      ? (activeIdx / (points.length - 1)) * viewWidth
      : viewWidth;

  const max = Math.max(...points, 1);
  const min = Math.min(...points, 0);
  const span = max - min || 1;
  const norm = active != null ? (active - min) / span : 0;
  const cursorY = viewHeight - norm * viewHeight;

  return (
    <div
      className="mini-trend"
      ref={containerRef}
      onPointerMove={onPointerMove}
      onPointerLeave={onPointerLeave}
    >
      <div className="mini-trend-head">
        <span>{label}</span>
        <span>{format(active)}</span>
      </div>
      <svg
        className="mini-trend-chart"
        viewBox={`0 0 ${viewWidth} ${viewHeight}`}
        preserveAspectRatio="none"
        onPointerMove={(e) => moveToPosition(e.clientX)}
        onPointerLeave={onPointerLeave}
      >
        <polyline
          fill="none"
          stroke={color}
          strokeWidth={1}
          strokeLinecap="round"
          points={coords}
        />
        {points.length ? (
          <>
            <line
              x1={cursorX}
              x2={cursorX}
              y1={0}
              y2={viewHeight}
              stroke={color}
              strokeWidth={0.4}
              strokeDasharray="2"
              opacity={0.4}
            />
            <circle
              cx={cursorX}
              cy={cursorY}
              r={2.2}
              fill="#04150f"
              stroke={color}
              strokeWidth={1}
            />
          </>
        ) : null}
      </svg>
    </div>
  );
}
