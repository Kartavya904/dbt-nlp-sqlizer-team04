export default function ProgressStrip({ value = 0, label = "" }) {
  const pct = Math.max(0, Math.min(100, Math.round(value)));
  return (
    <div>
      <div className="small" style={{ marginBottom: 6 }}>
        {label} <span className="badge">{pct}%</span>
      </div>
      <div className="progressOuter" title={`${pct}%`}>
        <div className="progressInner" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}
