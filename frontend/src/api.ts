import { useCallback, useEffect, useRef, useState } from "react";

export type Range = "15m" | "1h" | "6h" | "24h";

export interface Envelope<T> {
  range: string;
  since: string;
  generatedAt: string;
  dataStatus: string;
  data: T;
}

export interface Overview {
  totalEvents: number;
  pageViews: number;
  productViews: number;
  addToCart: number;
  checkoutsStarted: number;
  purchasesCompleted: number;
  purchasesFailed: number;
  revenue: string;
  conversionRate: number | null;
  latestWindowStart?: string;
  activeUsersLatestWindow?: number;
  latestWindowEventsPerSecond?: number;
}

export interface Point {
  windowStart: number;
  totalEvents: number;
  purchasesCompleted: number;
  revenue: string;
  activeUsers: number;
  windowClosed: number;
}

export interface DimRow {
  key: string;
  events: number;
  revenue: string;
}

export interface Pipeline {
  kafka: "UP" | "DOWN";
  clickhouse: "UP" | "DOWN";
  flinkJob: string;
  consumerLag: number | null;
  latestWindowStart?: string;
  lastSuccessfulUpdate: string | null;
  dataAgeSeconds: number | null;
  rejected: { INVALID: number; DUPLICATE: number; LATE: number } | null;
}

export interface State<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
}

/** Polls an endpoint. Keeps the last good data on a failed refresh and surfaces the error separately. */
export function usePolling<T>(path: string, intervalMs: number, enabled: boolean, tick: number): State<T> {
  const [state, setState] = useState<State<T>>({ data: null, error: null, loading: true });
  const seq = useRef(0);

  const load = useCallback(async () => {
    const id = ++seq.current;
    const ctl = new AbortController();
    const timeout = setTimeout(() => ctl.abort(), 8000);
    try {
      const res = await fetch(path, { signal: ctl.signal });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.detail ?? `HTTP ${res.status}`);
      }
      const json = (await res.json()) as T;
      if (id === seq.current) setState({ data: json, error: null, loading: false });
    } catch (e) {
      if (id === seq.current) {
        setState((s) => ({ data: s.data, error: e instanceof Error ? e.message : "request failed", loading: false }));
      }
    } finally {
      clearTimeout(timeout);
    }
  }, [path]);

  useEffect(() => {
    setState((s) => ({ ...s, loading: s.data === null }));
    load();
    if (!enabled) return;
    const t = setInterval(load, intervalMs);
    return () => clearInterval(t);
  }, [load, intervalMs, enabled, tick]);

  return state;
}
