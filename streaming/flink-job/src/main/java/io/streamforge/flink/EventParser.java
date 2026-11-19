package io.streamforge.flink;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Parses and semantically validates a raw Kafka record. The ingestion API enforces the same rules;
 * Flink re-validates because Kafka is a trust boundary (other producers, replays, old versions).
 */
public final class EventParser {
    static final Set<String> TYPES = Set.of("PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART",
            "CHECKOUT_STARTED", "PURCHASE_COMPLETED", "PURCHASE_FAILED");
    static final Set<String> DEVICES = Set.of("DESKTOP", "MOBILE", "TABLET");
    static final Duration MAX_FUTURE = Duration.ofMinutes(5);
    static final Duration MAX_PAST = Duration.ofDays(7);
    static final String CURRENCY = "USD";

    private EventParser() {}

    /** @throws IllegalArgumentException with a human-readable reason when the record is invalid. */
    public static Event parse(String json, Instant now) {
        JsonNode n;
        try {
            n = Json.MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("malformed JSON");
        }
        if (n == null || !n.isObject()) {
            throw new IllegalArgumentException("payload is not a JSON object");
        }
        Event e = new Event();
        e.eventId = text(n, "eventId", true);
        e.eventType = text(n, "eventType", true);
        e.userId = text(n, "userId", true);
        e.sessionId = text(n, "sessionId", true);
        e.deviceType = text(n, "deviceType", true);
        e.region = text(n, "region", true);
        e.source = text(n, "source", true);
        e.productId = text(n, "productId", false);
        e.category = text(n, "category", false);
        e.currency = text(n, "currency", false);

        if (!TYPES.contains(e.eventType)) {
            throw new IllegalArgumentException("unsupported eventType");
        }
        if (!DEVICES.contains(e.deviceType)) {
            throw new IllegalArgumentException("unsupported deviceType");
        }
        if (!n.hasNonNull("schemaVersion") || n.get("schemaVersion").asInt(-1) != 1) {
            throw new IllegalArgumentException("unsupported schemaVersion");
        }
        try {
            Instant ts = Instant.parse(text(n, "eventTimestamp", true));
            if (ts.isAfter(now.plus(MAX_FUTURE)) || ts.isBefore(now.minus(MAX_PAST))) {
                throw new IllegalArgumentException("eventTimestamp outside accepted range");
            }
            e.eventTimeMs = ts.toEpochMilli();
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("eventTimestamp is not ISO-8601 UTC");
        }
        if (n.hasNonNull("quantity")) {
            if (!n.get("quantity").isIntegralNumber() || n.get("quantity").asInt() < 1) {
                throw new IllegalArgumentException("quantity must be a positive integer");
            }
            e.quantity = n.get("quantity").asInt();
        }
        if (n.hasNonNull("amount")) {
            if (!n.get("amount").isNumber()) {
                throw new IllegalArgumentException("amount must be a number");
            }
            BigDecimal a = n.get("amount").decimalValue();
            if (a.signum() <= 0 || a.scale() > 2) {
                throw new IllegalArgumentException("amount must be positive with at most 2 decimals");
            }
            e.amount = a;
        }
        if (e.currency != null && !CURRENCY.equals(e.currency)) {
            throw new IllegalArgumentException("unsupported currency (only USD)");
        }
        switch (e.eventType) {
            case "PRODUCT_VIEW":
                require(e.productId != null && e.category != null, "productId and category required");
                break;
            case "ADD_TO_CART":
                require(e.productId != null && e.category != null && e.quantity != null,
                        "productId, category and quantity required");
                break;
            case "PURCHASE_COMPLETED":
                require(e.productId != null && e.category != null && e.quantity != null
                        && e.amount != null && CURRENCY.equals(e.currency),
                        "productId, category, quantity, amount and USD currency required");
                break;
            default:
                break;
        }
        return e;
    }

    /** Best-effort eventId extraction for rejected records; never throws. */
    public static String tryEventId(String json) {
        try {
            JsonNode n = Json.MAPPER.readTree(json);
            return n != null && n.hasNonNull("eventId") ? n.get("eventId").asText() : "";
        } catch (Exception e) {
            return "";
        }
    }

    private static void require(boolean ok, String msg) {
        if (!ok) {
            throw new IllegalArgumentException(msg);
        }
    }

    private static String text(JsonNode n, String field, boolean required) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || !v.isTextual() || v.asText().isBlank()) {
            if (required) {
                throw new IllegalArgumentException(field + " is required");
            }
            return null;
        }
        String s = v.asText();
        if (s.length() > 64) {
            throw new IllegalArgumentException(field + " too long");
        }
        return s;
    }
}
