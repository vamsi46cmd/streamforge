import { ReactNode, useMemo, useState } from "react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { DimRow, Envelope, Overview, Pipeline, Point, Range, State, usePolling } from "./api";

const RANGES: Range[] = ["15m", "1h", "6h", "24h"];
const COLORS = ["#2f6fed", "#12a594", "#f5a524", "#e5484d", "#8e4ec6", "#6b7280"];

const fmt = (n: number | undefined | null) => (n == null ? "n/a" : n.toLocaleString("en-US"));
const usd = (v: string | undefined) => (v == null ? "n/a" : Number(v).toLocaleString("en-US", { style: "currency", currency: "USD" }));
const hhmm = (epoch: number) => new Date(epoch * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

function Panel({ title, note, state, children, isEmpty }: {
  title: string; note?: string; state: State<unknown>; children: ReactNode; isEmpty?: boolean;
}) {
  return (
    <section className="panel" aria-busy={state.loading}>
      <header>
        <h3>{title}</h3>
        {note && <span className="note">{note}</span>}
      </header>
      {state.error && <p className="error" role="alert">Could not refresh: {state.error}</p>}
      {state.loading && !state.data ? <p className="muted">Loading…</p>
        : isEmpty ? <p className="muted">No data in this range yet. Start the generator to see live results.</p>
        : children}
    </section>
  );
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="stat">
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {hint && <span className="hint">{hint}</span>}
    </div>
  );
}

function Health({ label, ok, detail }: { label: string; ok: boolean | null; detail: string }) {
  const cls = ok === null ? "unknown" : ok ? "ok" : "bad";
  return (
    <div className="health">
      <span className={`dot ${cls}`} aria-hidden="true" />
      <span className="label">{label}</span>
      <span className="detail">{detail}</span>
    </div>
  );
}

export default function App() {
  const [range, setRange] = useState<Range>("1h");
  const [auto, setAuto] = useState(true);
  const [tick, setTick] = useState(0);
  const ov = usePolling<Envelope<Overview>>(`/api/v1/analytics/overview?range=${range}`, 5000, auto, tick);
  const ts = usePolling<Envelope<Point[]>>(`/api/v1/analytics/timeseries?range=${range}`, 5000, auto, tick);
  const cat = usePolling<Envelope<DimRow[]>>(`/api/v1/analytics/categories?range=${range}`, 5000, auto, tick);
  const reg = usePolling<Envelope<DimRow[]>>(`/api/v1/analytics/regions?range=${range}`, 5000, auto, tick);
  const dev = usePolling<Envelope<DimRow[]>>(`/api/v1/analytics/devices?range=${range}`, 5000, auto, tick);
  const top = usePolling<Envelope<DimRow[]>>(`/api/v1/analytics/top-products?range=${range}&limit=5`, 5000, auto, tick);
  const pl = usePolling<Envelope<Pipeline>>(`/api/v1/analytics/pipeline?range=${range}`, 5000, auto, tick);

  const series = useMemo(() => (ts.data?.data ?? []).map((p) => ({
    time: hhmm(p.windowStart),
    eventsPerSecond: Number((p.totalEvents / 60).toFixed(2)),
    revenue: Number(p.revenue),
    purchases: p.purchasesCompleted,
  })), [ts.data]);
  const dimData = (s: State<Envelope<DimRow[]>>) => (s.data?.data ?? []).map((r) => ({ name: r.key, events: r.events, revenue: Number(r.revenue) }));

  const o = ov.data?.data;
  const p = pl.data?.data;
  const lastUpdate = ov.data ? new Date(ov.data.generatedAt).toLocaleTimeString() : "never";

  return (
    <div className="app">
      <header className="top">
        <div>
          <h1>StreamForge</h1>
          <p className="muted">Real-time e-commerce analytics · Kafka → Flink → ClickHouse</p>
        </div>
        <div className="controls" role="group" aria-label="Dashboard controls">
          <label>
            <span className="sr">Time range</span>
            <select value={range} onChange={(e) => setRange(e.target.value as Range)} aria-label="Time range">
              {RANGES.map((r) => <option key={r} value={r}>Last {r}</option>)}
            </select>
          </label>
          <button onClick={() => setTick((t) => t + 1)}>Refresh</button>
          <label className="toggle">
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} /> Auto-refresh (5s)
          </label>
          <span className="muted">Updated {lastUpdate}</span>
        </div>
      </header>

      <h2>Overview</h2>
      <p className="muted small">Totals are sums of one-minute event-time windows over the selected range. {ov.data?.dataStatus}</p>
      {ov.error && <p className="error" role="alert">Overview unavailable: {ov.error}</p>}
      <div className="stats">
        <Stat label="Total events" value={fmt(o?.totalEvents)} hint="aggregate total" />
        <Stat label="Active users" value={fmt(o?.activeUsersLatestWindow)} hint="distinct users, latest minute" />
        <Stat label="Completed purchases" value={fmt(o?.purchasesCompleted)} hint="aggregate total" />
        <Stat label="Revenue (USD)" value={usd(o?.revenue)} hint="completed purchases" />
        <Stat label="Checkout conversion"
          value={o?.conversionRate == null ? "n/a" : `${(o.conversionRate * 100).toFixed(1)}%`}
          hint="purchases ÷ checkouts started" />
        <Stat label="Event rate"
          value={o?.latestWindowEventsPerSecond == null ? "n/a" : `${o.latestWindowEventsPerSecond} /s`}
          hint="latest minute (provisional)" />
      </div>

      <h2>Live analytics</h2>
      <div className="grid">
        <Panel title="Event rate" note="events per second, 1-minute average" state={ts} isEmpty={series.length === 0}>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={series}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="time" /><YAxis unit=" /s" width={60} />
              <Tooltip /><Line type="monotone" dataKey="eventsPerSecond" name="events/s" stroke={COLORS[0]} dot={false} /></LineChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Revenue over time" note="USD per minute" state={ts} isEmpty={series.length === 0}>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={series}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="time" /><YAxis width={70} tickFormatter={(v) => `$${v}`} />
              <Tooltip formatter={(v: number) => usd(String(v))} /><Area type="monotone" dataKey="revenue" name="revenue" stroke={COLORS[1]} fill={COLORS[1]} fillOpacity={0.2} /></AreaChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Purchase activity" note="completed purchases per minute" state={ts} isEmpty={series.length === 0}>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={series}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="time" /><YAxis allowDecimals={false} width={40} />
              <Tooltip /><Bar dataKey="purchases" name="purchases" fill={COLORS[2]} /></BarChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Events by category" note="event count" state={cat} isEmpty={dimData(cat).length === 0}>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={dimData(cat)} layout="vertical"><CartesianGrid strokeDasharray="3 3" /><XAxis type="number" allowDecimals={false} />
              <YAxis type="category" dataKey="name" width={90} /><Tooltip /><Bar dataKey="events" fill={COLORS[0]} /></BarChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Events by region" note="event count" state={reg} isEmpty={dimData(reg).length === 0}>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={dimData(reg)}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="name" /><YAxis allowDecimals={false} width={50} />
              <Tooltip /><Bar dataKey="events" fill={COLORS[3]} /></BarChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Events by device" note="share of events" state={dev} isEmpty={dimData(dev).length === 0}>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart><Pie data={dimData(dev)} dataKey="events" nameKey="name" innerRadius={50} outerRadius={80} label>
              {dimData(dev).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}</Pie><Tooltip /></PieChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Top products" note="by completed-purchase revenue" state={top} isEmpty={(top.data?.data ?? []).length === 0}>
          <table>
            <thead><tr><th>Product</th><th className="num">Events</th><th className="num">Revenue</th></tr></thead>
            <tbody>{(top.data?.data ?? []).map((r) => (
              <tr key={r.key}><td>{r.key}</td><td className="num">{fmt(r.events)}</td><td className="num">{usd(r.revenue)}</td></tr>))}</tbody>
          </table>
        </Panel>
      </div>

      <h2>Pipeline health</h2>
      <Panel title="Services and data flow" note="n/a = cannot be measured right now (not zero)" state={pl} isEmpty={false}>
        <div className="health-grid">
          <Health label="Kafka" ok={p ? p.kafka === "UP" : null} detail={p?.kafka ?? "unknown"} />
          <Health label="Flink job" ok={p ? (p.flinkJob === "RUNNING" ? true : p.flinkJob === "UNAVAILABLE" ? null : false) : null} detail={p?.flinkJob ?? "unknown"} />
          <Health label="ClickHouse" ok={p ? p.clickhouse === "UP" : null} detail={p?.clickhouse ?? "unknown"} />
          <Health label="Consumer lag" ok={p?.consumerLag == null ? null : p.consumerLag < 1000}
            detail={p?.consumerLag == null ? "n/a" : `${fmt(p.consumerLag)} records`} />
          <Health label="Data freshness" ok={p?.dataAgeSeconds == null ? null : p.dataAgeSeconds < 30}
            detail={p?.dataAgeSeconds == null ? "no data yet" : `${p.dataAgeSeconds}s since last window update`} />
          <Health label="Invalid events (DLQ)" ok={p?.rejected ? p.rejected.INVALID === 0 : null}
            detail={p?.rejected ? fmt(p.rejected.INVALID) : "n/a"} />
          <Health label="Late events (DLQ)" ok={p?.rejected ? p.rejected.LATE === 0 : null}
            detail={p?.rejected ? fmt(p.rejected.LATE) : "n/a"} />
          <Health label="Duplicates dropped" ok={p?.rejected ? true : null}
            detail={p?.rejected ? fmt(p.rejected.DUPLICATE) : "n/a"} />
        </div>
        <p className="muted small">Last successful data update: {p?.lastSuccessfulUpdate ? new Date(p.lastSuccessfulUpdate).toLocaleString() : "n/a"}</p>
      </Panel>
    </div>
  );
}
