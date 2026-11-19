package io.streamforge.flink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * ClickHouse sinks over the HTTP interface. Delivery is at-least-once: after a failure Flink restores the
 * last checkpoint and replays, so rows can be written twice. The ReplacingMergeTree tables make this harmless
 * for window results (same key + revision logic); rejected_events may contain duplicate rows after a replay.
 * Inserts are retried with bounded exponential backoff, then the task fails and Flink's restart strategy takes over.
 */
public final class ClickHouseSinks {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
    private static final int MAX_ATTEMPTS = 5;

    private ClickHouseSinks() {}

    abstract static class Base<T> extends RichSinkFunction<T> {
        private transient HttpClient client;
        private transient String url;
        private transient String auth;

        @Override
        public void open(Configuration parameters) {
            client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
            url = System.getenv().getOrDefault("CLICKHOUSE_URL", "http://clickhouse:8123");
            String user = System.getenv().getOrDefault("CLICKHOUSE_USER", "streamforge");
            String pass = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "streamforge-dev");
            auth = "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
        }

        void insert(String table, String jsonEachRow) throws IOException, InterruptedException {
            String query = URLEncoder.encode("INSERT INTO streamforge." + table + " FORMAT JSONEachRow", StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url + "/?query=" + query))
                    .timeout(Duration.ofSeconds(10))
                    .header("Authorization", auth)
                    .POST(HttpRequest.BodyPublishers.ofString(jsonEachRow))
                    .build();
            IOException last = null;
            long backoff = 200;
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                try {
                    HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
                    if (res.statusCode() == 200) {
                        return;
                    }
                    last = new IOException("ClickHouse insert into " + table + " failed: HTTP " + res.statusCode()
                            + " " + res.body().substring(0, Math.min(200, res.body().length())));
                    if (res.statusCode() >= 400 && res.statusCode() < 500) {
                        throw last; // client error: retrying cannot help
                    }
                } catch (IOException e) {
                    last = e;
                }
                if (attempt < MAX_ATTEMPTS) {
                    Thread.sleep(backoff);
                    backoff *= 2;
                }
            }
            throw last;
        }
    }

    public static class WindowSink extends Base<WindowResult> {
        @Override
        public void invoke(WindowResult r, Context ctx) throws Exception {
            String ws = TS.format(Instant.ofEpochMilli(r.windowStartMs));
            ObjectNode m = Json.MAPPER.createObjectNode();
            m.put("window_start", ws);
            m.put("total_events", r.totalEvents);
            m.put("page_views", r.pageViews);
            m.put("product_views", r.productViews);
            m.put("add_to_cart", r.addToCart);
            m.put("checkouts", r.checkouts);
            m.put("purchases", r.purchases);
            m.put("failed_purchases", r.failedPurchases);
            m.put("revenue", r.revenue);
            m.put("active_users", r.activeUsers);
            m.put("watermark_passed", r.watermarkPassed ? 1 : 0);
            m.put("revision", r.revision);
            insert("minute_metrics", m + "\n");

            StringBuilder sb = new StringBuilder();
            for (WindowResult.DimRow d : r.dims) {
                ObjectNode n = Json.MAPPER.createObjectNode();
                n.put("window_start", ws);
                n.put("dimension", d.dimension);
                n.put("key", d.key);
                n.put("events", d.events);
                n.put("revenue", d.revenue);
                n.put("revision", r.revision);
                sb.append(n).append('\n');
            }
            if (sb.length() > 0) {
                insert("dimension_metrics", sb.toString());
            }
        }
    }

    public static class RejectedSink extends Base<Rejected> {
        @Override
        public void invoke(Rejected r, Context ctx) throws Exception {
            ObjectNode n = Json.MAPPER.createObjectNode();
            n.put("ts", TS.format(Instant.ofEpochMilli(r.tsMs)));
            n.put("kind", r.kind);
            n.put("reason", r.reason);
            n.put("event_id", r.eventId);
            insert("rejected_events", n + "\n");
        }
    }
}
