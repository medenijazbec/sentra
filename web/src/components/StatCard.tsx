import { ReactNode } from "react";

type Props = {
  title: string;
  value: string;
  hint?: string;
  accent?: string;
  children?: ReactNode;
};

export function StatCard({ title, value, hint, accent, children }: Props) {
  return (
    <div className="card">
      <h3>{title}</h3>
      <p className="value" style={{ color: accent ?? "var(--text)" }}>
        {value}
      </p>
      {hint && <p className="hint">{hint}</p>}
      {children}
    </div>
  );
}
