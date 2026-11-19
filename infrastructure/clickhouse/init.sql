-- StreamForge analytics schema. Runs automatically on first start of an empty ClickHouse volume.
-- ReplacingMergeTree(revision): Flink re-emits a window every time it changes (provisional
-- firings and late-event updates). Each emission carries a strictly increasing `revision`;
-- the row with the highest revision wins, so replays/updates never double-count.
-- Always query with FINAL (or argMax) to read only the winning revision.

CREATE DATABASE IF NOT EXISTS streamforge;

CREATE TABLE IF NOT EXISTS streamforge.minute_metrics
(
    window_start     DateTime('UTC'),
    total_events     UInt64,
    page_views       UInt64,
    product_views    UInt64,
    add_to_cart      UInt64,
    checkouts        UInt64,
    purchases        UInt64,
    failed_purchases UInt64,
    revenue          Decimal(18, 2),
    active_users     UInt64,
    watermark_passed UInt8,
    revision         UInt64
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(window_start)
ORDER BY window_start
TTL window_start + INTERVAL 30 DAY;

-- dimension: 'category' | 'region' | 'device' | 'product'
CREATE TABLE IF NOT EXISTS streamforge.dimension_metrics
(
    window_start DateTime('UTC'),
    dimension    LowCardinality(String),
    key          String,
    events       UInt64,
    revenue      Decimal(18, 2),
    revision     UInt64
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(window_start)
ORDER BY (dimension, window_start, key)
TTL window_start + INTERVAL 30 DAY;

-- kind: 'INVALID' | 'DUPLICATE' | 'LATE'. Plain MergeTree: one row per rejected event.
CREATE TABLE IF NOT EXISTS streamforge.rejected_events
(
    ts       DateTime('UTC'),
    kind     LowCardinality(String),
    reason   String,
    event_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (kind, ts)
TTL ts + INTERVAL 14 DAY;
