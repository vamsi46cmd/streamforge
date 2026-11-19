package io.streamforge.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamforge.api.Errors.DependencyUnavailableException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Minimal ClickHouse HTTP client. All values go through ClickHouse server-side query parameters
 * ({name:Type} placeholders + param_name URL args), never string concatenation, so there is no SQL injection surface.
 */
@Component
public class ClickHouseClient {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final ObjectMapper mapper;
    private final String url;
    private final String auth;

    public ClickHouseClient(Config.Props props, ObjectMapper mapper) {
        this.mapper = mapper;
        this.url = props.clickhouse().url();
        this.auth = "Basic " + Base64.getEncoder().encodeToString(
                (props.clickhouse().user() + ":" + props.clickhouse().password()).getBytes(StandardCharsets.UTF_8));
    }

    /** Runs a SELECT and returns the "data" array (decimals as exact strings, 64-bit ints as numbers). */
    public JsonNode query(String sql, Map<String, String> params) {
        StringBuilder q = new StringBuilder(url)
                .append("/?default_format=JSON&output_format_json_quote_64bit_integers=0")
                .append("&output_format_json_quote_decimals=1&max_execution_time=5&max_result_rows=10000");
        params.forEach((k, v) -> q.append("&param_").append(k).append('=')
                .append(URLEncoder.encode(v, StandardCharsets.UTF_8)));
        HttpRequest req = HttpRequest.newBuilder(URI.create(q.toString()))
                .timeout(Duration.ofSeconds(8))
                .header("Authorization", auth)
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build();
        try {
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                throw new DependencyUnavailableException("Analytics store returned HTTP " + res.statusCode(), null);
            }
            return mapper.readTree(res.body()).get("data");
        } catch (DependencyUnavailableException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DependencyUnavailableException("Interrupted while querying the analytics store", e);
        } catch (Exception e) {
            throw new DependencyUnavailableException("Analytics store (ClickHouse) is unreachable", e);
        }
    }

    public boolean ping() {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url + "/ping")).timeout(Duration.ofSeconds(2)).GET().build();
            return http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }
}
