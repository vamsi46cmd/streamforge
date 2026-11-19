package io.streamforge.flink;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/** Pure aggregation logic for one window; unit-testable without a Flink runtime. */
public final class Aggregation {
    private static final AtomicLong LAST_REVISION = new AtomicLong();

    private Aggregation() {}

    /** Strictly increasing within a JVM (millis*1000 + counter), so a later firing always wins in ClickHouse. */
    public static long nextRevision() {
        return LAST_REVISION.updateAndGet(prev -> Math.max(prev + 1, System.currentTimeMillis() * 1000L));
    }

    public static WindowResult aggregate(long windowStartMs, Iterable<Event> events, boolean watermarkPassed) {
        WindowResult r = new WindowResult();
        r.windowStartMs = windowStartMs;
        r.watermarkPassed = watermarkPassed;
        Set<String> users = new HashSet<>();
        Map<String, long[]> counts = new TreeMap<>();
        Map<String, BigDecimal> revenue = new TreeMap<>();
        BigDecimal total = BigDecimal.ZERO;

        for (Event e : events) {
            r.totalEvents++;
            users.add(e.userId);
            boolean purchase = "PURCHASE_COMPLETED".equals(e.eventType);
            BigDecimal amount = purchase && e.amount != null ? e.amount : BigDecimal.ZERO;
            switch (e.eventType) {
                case "PAGE_VIEW": r.pageViews++; break;
                case "PRODUCT_VIEW": r.productViews++; break;
                case "ADD_TO_CART": r.addToCart++; break;
                case "CHECKOUT_STARTED": r.checkouts++; break;
                case "PURCHASE_COMPLETED": r.purchases++; break;
                case "PURCHASE_FAILED": r.failedPurchases++; break;
                default: break;
            }
            total = total.add(amount);
            bump(counts, revenue, "category", e.category, amount);
            bump(counts, revenue, "product", e.productId, amount);
            bump(counts, revenue, "region", e.region, amount);
            bump(counts, revenue, "device", e.deviceType, amount);
        }
        r.revenue = total;
        r.activeUsers = users.size();
        List<WindowResult.DimRow> rows = new ArrayList<>();
        for (Map.Entry<String, long[]> en : counts.entrySet()) {
            int sep = en.getKey().indexOf('|');
            rows.add(new WindowResult.DimRow(en.getKey().substring(0, sep), en.getKey().substring(sep + 1),
                    en.getValue()[0], revenue.get(en.getKey())));
        }
        r.dims = rows.toArray(new WindowResult.DimRow[0]);
        r.revision = nextRevision();
        return r;
    }

    private static void bump(Map<String, long[]> counts, Map<String, BigDecimal> rev,
                             String dim, String key, BigDecimal amount) {
        if (key == null) {
            return;
        }
        String k = dim + "|" + key;
        counts.computeIfAbsent(k, x -> new long[1])[0]++;
        rev.merge(k, amount, BigDecimal::add);
    }
}
