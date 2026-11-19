package io.streamforge.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Health")
public class HealthController {
    private final PipelineStatus pipeline;
    private final ClickHouseClient clickhouse;

    public HealthController(PipelineStatus pipeline, ClickHouseClient clickhouse) {
        this.pipeline = pipeline;
        this.clickhouse = clickhouse;
    }

    @GetMapping("/health")
    @Operation(summary = "Liveness: the process is up.")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }

    @GetMapping("/ready")
    @Operation(summary = "Readiness: Kafka and ClickHouse reachable. 503 otherwise.")
    public ResponseEntity<Map<String, Object>> ready() {
        boolean kafka = pipeline.kafkaReachable();
        boolean ch = clickhouse.ping();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", kafka && ch ? "READY" : "NOT_READY");
        body.put("kafka", kafka ? "UP" : "DOWN");
        body.put("clickhouse", ch ? "UP" : "DOWN");
        return ResponseEntity.status(kafka && ch ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
