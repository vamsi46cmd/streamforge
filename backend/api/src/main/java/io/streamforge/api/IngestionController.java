package io.streamforge.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.streamforge.api.Errors.DependencyUnavailableException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.Validator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ingestion. A 202 means Kafka acknowledged the record with acks=all (idempotent producer, bounded retries).
 * It does NOT mean Flink processed it or that analytics reflect it yet.
 */
@RestController
@RequestMapping("/api/v1/events")
@Tag(name = "Ingestion")
public class IngestionController {
    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);
    static final int MAX_BATCH = 500;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Validator validator;
    private final Config.Props props;
    private final Counter accepted;
    private final Counter rejected;
    private final Counter publishFailures;
    private final Timer latency;

    public IngestionController(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Validator validator,
                               Config.Props props, MeterRegistry registry) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.validator = validator;
        this.props = props;
        this.accepted = registry.counter("streamforge.ingestion.accepted");
        this.rejected = registry.counter("streamforge.ingestion.rejected");
        this.publishFailures = registry.counter("streamforge.ingestion.publish.failures");
        this.latency = registry.timer("streamforge.ingestion.latency");
    }

    @PostMapping
    @Operation(summary = "Ingest one event",
            description = "202 once Kafka has acknowledged the record (acks=all). 400 invalid, 503 Kafka unavailable.")
    public ResponseEntity<Map<String, Object>> ingest(@Valid @RequestBody EventRequest event) {
        List<String> errors = EventRules.check(event, Instant.now());
        if (!errors.isEmpty()) {
            rejected.increment();
            throw new ValidationFailure(errors);
        }
        Timer.Sample sample = Timer.start();
        try {
            SendResult<String, String> r = send(event).get(props.kafka().publishTimeoutMs(), TimeUnit.MILLISECONDS);
            accepted.increment();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("eventId", event.eventId());
            body.put("status", "ACCEPTED");
            body.put("partition", r.getRecordMetadata().partition());
            body.put("offset", r.getRecordMetadata().offset());
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
        } catch (Exception e) {
            publishFailures.increment();
            log.warn("Kafka publish failed: {}", e.toString());
            throw new DependencyUnavailableException("Event was NOT durably accepted: Kafka did not acknowledge. Safe to retry.", e);
        } finally {
            sample.stop(latency);
        }
    }

    @PostMapping("/batch")
    @Operation(summary = "Ingest up to 500 events",
            description = "Valid events are published; invalid ones are reported per index. 202 if some were accepted and none failed; 400 if none valid; 503 if any publish failed (response lists which indexes).")
    public ResponseEntity<Map<String, Object>> ingestBatch(@RequestBody List<JsonNode> items) {
        if (items == null || items.isEmpty()) {
            throw new IllegalArgumentException("Body must be a non-empty JSON array of events.");
        }
        if (items.size() > MAX_BATCH) {
            throw new IllegalArgumentException("Batch too large: max " + MAX_BATCH + " events.");
        }
        Instant now = Instant.now();
        List<Map<String, Object>> rejections = new ArrayList<>();
        Map<Integer, CompletableFuture<SendResult<String, String>>> inflight = new LinkedHashMap<>();
        Timer.Sample sample = Timer.start();

        for (int i = 0; i < items.size(); i++) {
            List<String> errors = new ArrayList<>();
            EventRequest e = null;
            try {
                e = mapper.treeToValue(items.get(i), EventRequest.class);
                validator.validate(e).forEach(v -> errors.add(v.getPropertyPath() + ": " + v.getMessage()));
                errors.addAll(EventRules.check(e, now));
            } catch (Exception ex) {
                errors.add("unreadable event (bad JSON shape, enum value or timestamp)");
            }
            if (!errors.isEmpty()) {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("index", i);
                r.put("errors", errors);
                rejections.add(r);
                rejected.increment();
            } else {
                try {
                    inflight.put(i, send(e));
                } catch (Exception ex) {
                    inflight.put(i, CompletableFuture.failedFuture(ex));
                }
            }
        }

        int ok = 0;
        List<Integer> failed = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(props.kafka().publishTimeoutMs());
        for (Map.Entry<Integer, CompletableFuture<SendResult<String, String>>> en : inflight.entrySet()) {
            try {
                en.getValue().get(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())), TimeUnit.MILLISECONDS);
                ok++;
                accepted.increment();
            } catch (Exception ex) {
                failed.add(en.getKey());
                publishFailures.increment();
            }
        }
        sample.stop(latency);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("received", items.size());
        body.put("accepted", ok);
        body.put("rejected", rejections.size());
        body.put("failedToPublish", failed);
        body.put("rejections", rejections);
        HttpStatus status = !failed.isEmpty() ? HttpStatus.SERVICE_UNAVAILABLE
                : ok == 0 ? HttpStatus.BAD_REQUEST : HttpStatus.ACCEPTED;
        if (!failed.isEmpty()) {
            log.warn("Batch publish: {} of {} events not acknowledged by Kafka", failed.size(), inflight.size());
        }
        return ResponseEntity.status(status).body(body);
    }

    private CompletableFuture<SendResult<String, String>> send(EventRequest e) throws Exception {
        // Key = sessionId: events of one session stay ordered within a partition.
        return kafka.send(props.topics().raw(), e.sessionId(), mapper.writeValueAsString(e));
    }

    /** Carries per-field messages for the cross-field rules. */
    public static class ValidationFailure extends RuntimeException {
        final List<String> errors;

        ValidationFailure(List<String> errors) {
            super("Validation failed");
            this.errors = errors;
        }
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(ValidationFailure.class)
    org.springframework.http.ProblemDetail validation(ValidationFailure ex) {
        return Errors.problem(HttpStatus.BAD_REQUEST, "Validation failed", "The event is invalid.", ex.errors);
    }
}
