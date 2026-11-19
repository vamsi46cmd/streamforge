#!/usr/bin/env python3
"""StreamForge event generator. Sends realistic e-commerce sessions through the real ingestion API.

Examples:
  python3 generator/generate.py --rate 50 --duration 120 --seed 42
  python3 generator/generate.py --rate 20 --duplicate-rate 0.05 --invalid-rate 0.02 --late-rate 0.03

Stop with Ctrl+C (graceful: flushes the in-flight batch, prints exact sent/acknowledged counts).
Only the Python standard library is used.
"""
import argparse
import json
import random
import signal
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

CATEGORIES = {"electronics": 0.30, "fashion": 0.25, "home": 0.20, "books": 0.15, "toys": 0.10}
REGIONS = {"US-EAST": 0.35, "US-WEST": 0.25, "EU-WEST": 0.25, "APAC": 0.15}
DEVICES = {"MOBILE": 0.55, "DESKTOP": 0.38, "TABLET": 0.07}
PRICE_RANGE = {"electronics": (30, 900), "fashion": (10, 150), "home": (8, 300), "books": (5, 40), "toys": (6, 60)}

stop = False


def on_signal(*_):
    global stop
    stop = True


def pick(rng, weights):
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Catalog:
    def __init__(self, rng, size):
        self.items = []
        for i in range(size):
            cat = pick(rng, CATEGORIES)
            lo, hi = PRICE_RANGE[cat]
            self.items.append((f"prod-{i:04d}", cat, round(rng.uniform(lo, hi), 2)))

    def random(self, rng):
        return rng.choice(self.items)


def make_session(rng, catalog, users, conversion):
    """Yields the event bodies (without id/timestamp) of one realistic session."""
    user = f"user-{rng.randrange(users):06d}"
    session = f"sess-{uuid.UUID(int=rng.getrandbits(128)).hex[:16]}"
    base = {"userId": user, "sessionId": session, "deviceType": pick(rng, DEVICES),
            "region": pick(rng, REGIONS), "source": rng.choice(["web", "ios", "android"]), "schemaVersion": 1}
    seq = [dict(base, eventType="PAGE_VIEW")]
    for _ in range(rng.randint(1, 4)):
        pid, cat, price = catalog.random(rng)
        seq.append(dict(base, eventType="PRODUCT_VIEW", productId=pid, category=cat))
        if rng.random() < 0.35:
            qty = rng.randint(1, 3)
            seq.append(dict(base, eventType="ADD_TO_CART", productId=pid, category=cat, quantity=qty))
            if rng.random() < 0.55:
                seq.append(dict(base, eventType="CHECKOUT_STARTED"))
                if rng.random() < conversion:
                    seq.append(dict(base, eventType="PURCHASE_COMPLETED", productId=pid, category=cat, quantity=qty,
                                    amount=f"{price * qty:.2f}", currency="USD"))
                else:
                    seq.append(dict(base, eventType="PURCHASE_FAILED"))
                break
    return seq


def post_batch(url, events, timeout=10):
    req = urllib.request.Request(url + "/api/v1/events/batch", data=json.dumps(events).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception as e:  # connection refused, timeout...
        return 0, {"error": str(e)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--rate", type=float, default=20, help="events per second")
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = until Ctrl+C")
    ap.add_argument("--users", type=int, default=2000)
    ap.add_argument("--products", type=int, default=200)
    ap.add_argument("--conversion", type=float, default=0.35, help="P(purchase | checkout started)")
    ap.add_argument("--duplicate-rate", type=float, default=0.0)
    ap.add_argument("--invalid-rate", type=float, default=0.0)
    ap.add_argument("--out-of-order-rate", type=float, default=0.05, help="events delayed 1-8s (within the 10s watermark)")
    ap.add_argument("--late-rate", type=float, default=0.0, help="events delayed 15-90s (inside 2 min allowed lateness)")
    ap.add_argument("--too-late-rate", type=float, default=0.0, help="events delayed 4-6 min (beyond allowed lateness -> LATE)")
    ap.add_argument("--seed", type=int, default=None, help="deterministic random seed")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    catalog = Catalog(rng, args.products)
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    pending = []
    sent = acked = rejected = failed = injected_dup = injected_invalid = 0
    start = time.monotonic()
    tick = 0.25
    budget = 0.0
    print(f"generating ~{args.rate:g} events/s to {args.url} (seed={args.seed}); Ctrl+C to stop", flush=True)

    def flush(batch):
        nonlocal acked, rejected, failed
        for i in range(0, len(batch), 200):
            chunk = batch[i:i + 200]
            status, body = post_batch(args.url, chunk)
            if status == 202 or status == 400:
                acked += body.get("accepted", 0)
                rejected += body.get("rejected", 0)
            else:
                failed += len(chunk)

    last_report = start
    while not stop and (args.duration == 0 or time.monotonic() - start < args.duration):
        loop_start = time.monotonic()
        budget += args.rate * tick
        batch = []
        while budget >= 1:
            if not pending:
                pending = make_session(rng, catalog, args.users, args.conversion)
            body = pending.pop(0)
            now = datetime.now(timezone.utc)
            r = rng.random()
            if r < args.too_late_rate:
                ts = now - timedelta(seconds=rng.uniform(240, 360))
            elif r < args.too_late_rate + args.late_rate:
                ts = now - timedelta(seconds=rng.uniform(15, 90))
            elif r < args.too_late_rate + args.late_rate + args.out_of_order_rate:
                ts = now - timedelta(seconds=rng.uniform(1, 8))
            else:
                ts = now
            event = dict(body, eventId=f"evt-{uuid.UUID(int=rng.getrandbits(128)).hex}", eventTimestamp=iso(ts))
            if rng.random() < args.invalid_rate:
                event["eventType"] = rng.choice(["NOT_A_TYPE", "PURCHASE_COMPLETED"])  # unknown type / missing amount
                event.pop("amount", None)
                injected_invalid += 1
            batch.append(event)
            if rng.random() < args.duplicate_rate:
                batch.append(dict(event))
                injected_dup += 1
            budget -= 1
        if batch:
            sent += len(batch)
            flush(batch)
        if time.monotonic() - last_report >= 5:
            print(f"  t={time.monotonic() - start:5.0f}s sent={sent} acked={acked} rejected={rejected} failed={failed}", flush=True)
            last_report = time.monotonic()
        time.sleep(max(0, tick - (time.monotonic() - loop_start)))

    elapsed = time.monotonic() - start
    print(json.dumps({"seconds": round(elapsed, 1), "sent": sent, "acknowledgedByKafka": acked,
                      "rejectedByApi": rejected, "failedToPublish": failed,
                      "injectedDuplicates": injected_dup, "injectedInvalid": injected_invalid,
                      "avgSentPerSecond": round(sent / elapsed, 1) if elapsed else 0}, indent=2))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
