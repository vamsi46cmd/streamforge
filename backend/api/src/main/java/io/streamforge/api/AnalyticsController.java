package io.streamforge.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;
import java.util.OptionalLong;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read API over ClickHouse. All numbers come from ClickHouse via FINAL (winning revision per window).
 * dataStatus: windows at the leading edge are PROVISIONAL (refreshed every ~5s); older windows are final for
 * on-time data but can still be revised by late events up to 2 minutes (+10s watermark delay) after window end.
 */
@RestController
@RequestMapping("/api/v1/analytics")
@Tag(name = "Analytics")
public class AnalyticsController {
    static final String DATA_STATUS =
            "Latest windows are provisional and change until closed; closed windows can be revised by late events for up to ~2 minutes.";
    private static final Map<String, Integer> RANGES = Map.of("15m", 15, "1h", 60, "6h", 360, "24h", 1440);

    private final ClickHouseClient ch;
    private final PipelineStatus status;
    private final ObjectMapper mapper;

    public AnalyticsController(ClickHouseClient ch, PipelineStatus status, ObjectMapper mapper) {
        this.ch = ch;
        this.status = status;
        this.mapper = mapper;
    }

    private static long since(String range) {
        Integer minutes = RANGES.get(range);
        if (minutes == null) {
            throw new IllegalArgumentException("range must be one of 15m, 1h, 6h, 24h");
        }
        return Instant.now().getEpochSecond() / 60 * 60 - minutes * 60L;
    }

    private ObjectNode envelope(String range, long since, JsonNode data) {
        ObjectNode n = mapper.createObjectNode();
        n.put("range", range);
        n.put("since", Instant.ofEpochSecond(since).toString());
        n.put("generatedAt", Instant.now().toString());
        n.put("dataStatus", DATA_STATUS);
        n.set("data", data);
        return n;
    }

    @GetMapping("/overview")
    @Operation(summary = "KPI totals over the range. conversionRate = purchasesCompleted / checkoutsStarted.")
    public JsonNode overview(@RequestParam(defaultValue = "1h") String range) {
        long since = since(range);
        Map<String, String> p = Map.of("since", Long.toString(since));
        JsonNode totals = ch.query("""
                SELECT sum(total_events) AS totalEvents, sum(page_views) AS pageViews, sum(product_views) AS productViews,
                       sum(add_to_cart) AS addToCart, sum(checkouts) AS checkoutsStarted, sum(purchases) AS purchasesCompleted,
                       sum(failed_purchases) AS purchasesFailed, sum(revenue) AS revenue
                FROM streamforge.minute_metrics FINAL
                WHERE window_start >= toDateTime({since:UInt32}, 'UTC')""", p).get(0);
        JsonNode latest = ch.query("""
                SELECT toUnixTimestamp(window_start) AS windowStart, active_users AS activeUsers, total_events AS events
                FROM streamforge.minute_metrics FINAL
                WHERE window_start >= toDateTime({since:UInt32}, 'UTC')
                ORDER BY window_start DESC LIMIT 1""", p);
        ObjectNode data = ((ObjectNode) totals).deepCopy();
        long checkouts = totals.path("checkoutsStarted").asLong();
        if (checkouts > 0) {
            data.put("conversionRate", BigDecimal.valueOf(totals.path("purchasesCompleted").asLong())
                    .divide(BigDecimal.valueOf(checkouts), 4, RoundingMode.HALF_UP));
        } else {
            data.putNull("conversionRate"); // undefined, not zero
        }
        if (latest.size() > 0) {
            data.put("latestWindowStart", Instant.ofEpochSecond(latest.get(0).path("windowStart").asLong()).toString());
            data.put("activeUsersLatestWindow", latest.get(0).path("activeUsers").asLong());
            data.put("latestWindowEventsPerSecond", BigDecimal.valueOf(latest.get(0).path("events").asLong())
                    .divide(BigDecimal.valueOf(60), 2, RoundingMode.HALF_UP));
        }
        return envelope(range, since, data);
    }

    @GetMapping("/timeseries")
    @Operation(summary = "Per-minute series (one row per one-minute event-time window).")
    public JsonNode timeseries(@RequestParam(defaultValue = "1h") String range) {
        long since = since(range);
        JsonNode rows = ch.query("""
                SELECT toUnixTimestamp(window_start) AS windowStart, total_events AS totalEvents, page_views AS pageViews,
                       product_views AS productViews, add_to_cart AS addToCart, checkouts AS checkoutsStarted,
                       purchases AS purchasesCompleted, failed_purchases AS purchasesFailed, revenue,
                       active_users AS activeUsers, watermark_passed AS windowClosed
                FROM streamforge.minute_metrics FINAL
                WHERE window_start >= toDateTime({since:UInt32}, 'UTC')
                ORDER BY window_start""", Map.of("since", Long.toString(since)));
        return envelope(range, since, rows);
    }

    @GetMapping("/categories")
    public JsonNode categories(@RequestParam(defaultValue = "1h") String range,
                               @RequestParam(defaultValue = "10") int limit) {
        return dimension("category", "events", range, limit);
    }

    @GetMapping("/regions")
    public JsonNode regions(@RequestParam(defaultValue = "1h") String range,
                            @RequestParam(defaultValue = "10") int limit) {
        return dimension("region", "events", range, limit);
    }

    @GetMapping("/devices")
    public JsonNode devices(@RequestParam(defaultValue = "1h") String range,
                            @RequestParam(defaultValue = "10") int limit) {
        return dimension("device", "events", range, limit);
    }

    @GetMapping("/top-products")
    @Operation(summary = "Top products by completed-purchase revenue.")
    public JsonNode topProducts(@RequestParam(defaultValue = "1h") String range,
                                @RequestParam(defaultValue = "10") int limit) {
        return dimension("product", "revenue", range, limit);
    }

    private JsonNode dimension(String dim, String orderBy, String range, int limit) {
        if (limit < 1 || limit > 50) {
            throw new IllegalArgumentException("limit must be between 1 and 50");
        }
        long since = since(range);
        String order = "revenue".equals(orderBy) ? "revenue DESC, events DESC" : "events DESC";
        JsonNode rows = ch.query("""
                SELECT key, sum(events) AS events, sum(revenue) AS revenue
                FROM streamforge.dimension_metrics FINAL
                WHERE dimension = {dim:String} AND window_start >= toDateTime({since:UInt32}, 'UTC')
                GROUP BY key ORDER BY\s""" + order + " LIMIT {limit:UInt32}",
                Map.of("dim", dim, "since", Long.toString(since), "limit", Integer.toString(limit)));
        return envelope(range, since, rows);
    }

    @GetMapping("/pipeline")
    @Operation(summary = "Pipeline health. Fields that cannot be measured are null/UNAVAILABLE, never a fake 0.")
    public JsonNode pipeline(@RequestParam(defaultValue = "1h") String range) {
        long since = since(range);
        ObjectNode data = mapper.createObjectNode();

        boolean kafkaUp = status.kafkaReachable();
        data.put("kafka", kafkaUp ? "UP" : "DOWN");
        OptionalLong lag = status.consumerLag();
        if (lag.isPresent()) {
            data.put("consumerLag", lag.getAsLong());
        } else {
            data.putNull("consumerLag");
        }
        String flink = status.flinkJobState();
        data.put("flinkJob", flink == null ? "UNAVAILABLE" : flink);

        try {
            JsonNode fresh = ch.query("""
                    SELECT toUnixTimestamp(max(window_start)) AS latestWindow, intDiv(max(revision), 1000000) AS updatedAt, count() AS windows
                    FROM streamforge.minute_metrics""", Map.of()).get(0);
            data.put("clickhouse", "UP");
            if (fresh.path("windows").asLong() > 0) {
                long updated = fresh.path("updatedAt").asLong();
                data.put("latestWindowStart", Instant.ofEpochSecond(fresh.path("latestWindow").asLong()).toString());
                data.put("lastSuccessfulUpdate", Instant.ofEpochSecond(updated).toString());
                data.put("dataAgeSeconds", Math.max(0, Instant.now().getEpochSecond() - updated));
            } else {
                data.putNull("lastSuccessfulUpdate");
                data.putNull("dataAgeSeconds");
            }
            ObjectNode rej = mapper.createObjectNode().put("INVALID", 0).put("DUPLICATE", 0).put("LATE", 0);
            for (JsonNode r : ch.query("""
                    SELECT kind, count() AS count FROM streamforge.rejected_events
                    WHERE ts >= toDateTime({since:UInt32}, 'UTC') GROUP BY kind""", Map.of("since", Long.toString(since)))) {
                rej.put(r.path("kind").asText(), r.path("count").asLong());
            }
            data.set("rejected", rej);
        } catch (Errors.DependencyUnavailableException e) {
            data.put("clickhouse", "DOWN");
            data.putNull("lastSuccessfulUpdate");
            data.putNull("dataAgeSeconds");
            data.putNull("rejected");
        }
        return envelope(range, since, data);
    }
}
