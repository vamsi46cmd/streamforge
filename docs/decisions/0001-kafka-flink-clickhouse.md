# ADR 0001: Kafka + Flink + ClickHouse

## Status
Accepted.

## Context
We need to ingest bursty event streams, compute event-time aggregates that tolerate duplicates and late data,
and serve fast analytical queries to a dashboard.

## Decision
- **Kafka (KRaft, single broker locally)** is the durable buffer between ingestion and processing. It decouples API
  availability from processing, allows replay after failures, and gives ordered partitions (key = `sessionId`).
- **Flink (DataStream API)** does the processing because it has first-class event time, watermarks, allowed lateness,
  keyed state with TTL, and checkpointed recovery, which are the requirements here. A scheduled batch job or plain
  Java streams would not give correct late-event and recovery behavior.
- **ClickHouse** serves the analytics: columnar storage makes time-range aggregations cheap, and
  `ReplacingMergeTree(revision)` lets Flink re-emit a window repeatedly (provisional results, late updates, replays)
  without double counting.

## Consequences
- Window results are idempotent writes keyed by window with a revision; rejected-event rows are not idempotent.
- Analytics are eventually consistent: recent windows are provisional and can be revised for ~2 minutes.
- Three stateful systems must be run and monitored; a single-node dev setup demonstrates behavior, not production capacity.
- We do not claim end-to-end exactly-once; see README "Delivery guarantees".
