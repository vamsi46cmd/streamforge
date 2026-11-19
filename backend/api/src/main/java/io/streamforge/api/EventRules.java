package io.streamforge.api;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Event-type-specific and cross-field validation. Mirrors the rules Flink re-applies on the stream. */
public final class EventRules {
    public static final int SCHEMA_VERSION = 1;
    public static final String CURRENCY = "USD";
    static final Duration MAX_FUTURE = Duration.ofMinutes(5);
    static final Duration MAX_PAST = Duration.ofDays(7);

    private EventRules() {}

    public static List<String> check(EventRequest e, Instant now) {
        List<String> errors = new ArrayList<>();
        if (e.schemaVersion() != null && e.schemaVersion() != SCHEMA_VERSION) {
            errors.add("schemaVersion: only version " + SCHEMA_VERSION + " is supported");
        }
        if (e.eventTimestamp() != null) {
            if (e.eventTimestamp().isAfter(now.plus(MAX_FUTURE))) {
                errors.add("eventTimestamp: more than 5 minutes in the future");
            }
            if (e.eventTimestamp().isBefore(now.minus(MAX_PAST))) {
                errors.add("eventTimestamp: older than 7 days");
            }
        }
        if (e.currency() != null && !CURRENCY.equals(e.currency())) {
            errors.add("currency: only USD is supported");
        }
        if (e.eventType() == null) {
            return errors;
        }
        switch (e.eventType()) {
            case PRODUCT_VIEW -> {
                need(errors, e.productId(), "productId");
                need(errors, e.category(), "category");
            }
            case ADD_TO_CART -> {
                need(errors, e.productId(), "productId");
                need(errors, e.category(), "category");
                if (e.quantity() == null) {
                    errors.add("quantity: required for ADD_TO_CART");
                }
            }
            case PURCHASE_COMPLETED -> {
                need(errors, e.productId(), "productId");
                need(errors, e.category(), "category");
                if (e.quantity() == null) {
                    errors.add("quantity: required for PURCHASE_COMPLETED");
                }
                if (e.amount() == null) {
                    errors.add("amount: required for PURCHASE_COMPLETED");
                }
                if (!CURRENCY.equals(e.currency())) {
                    errors.add("currency: USD required for PURCHASE_COMPLETED");
                }
            }
            default -> { }
        }
        return errors;
    }

    private static void need(List<String> errors, String v, String name) {
        if (v == null || v.isBlank()) {
            errors.add(name + ": required for this eventType");
        }
    }
}
