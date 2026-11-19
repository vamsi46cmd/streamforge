package io.streamforge.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamforge.api.EventRequest.DeviceType;
import io.streamforge.api.EventRequest.EventType;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventRulesTest {
    private static final Instant NOW = Instant.parse("2026-11-18T12:00:00Z");
    private static final Validator V = Validation.buildDefaultValidatorFactory().getValidator();

    private static EventRequest event(EventType type, String product, String category, Integer qty,
                                      BigDecimal amount, String currency, Instant ts) {
        return new EventRequest("evt-00000001", type, ts, "u1", "s1", product, category, qty, amount, currency,
                DeviceType.MOBILE, "US-EAST", "web", 1);
    }

    @Test
    void validPurchasePasses() {
        EventRequest e = event(EventType.PURCHASE_COMPLETED, "p1", "books", 1, new BigDecimal("19.99"), "USD", NOW);
        assertTrue(V.validate(e).isEmpty());
        assertTrue(EventRules.check(e, NOW).isEmpty());
    }

    @Test
    void purchaseNeedsAmountQuantityAndUsd() {
        var errors = EventRules.check(event(EventType.PURCHASE_COMPLETED, "p1", "books", null, null, "EUR", NOW), NOW);
        assertTrue(errors.stream().anyMatch(s -> s.startsWith("amount")));
        assertTrue(errors.stream().anyMatch(s -> s.startsWith("quantity")));
        assertTrue(errors.stream().anyMatch(s -> s.startsWith("currency")));
    }

    @Test
    void pageViewNeedsNoProductFields() {
        assertTrue(EventRules.check(event(EventType.PAGE_VIEW, null, null, null, null, null, NOW), NOW).isEmpty());
    }

    @Test
    void productViewNeedsProductAndCategory() {
        assertEquals(2, EventRules.check(event(EventType.PRODUCT_VIEW, null, null, null, null, null, NOW), NOW).size());
    }

    @Test
    void timestampBoundsAreEnforced() {
        assertEquals(1, EventRules.check(event(EventType.PAGE_VIEW, null, null, null, null, null, NOW.plusSeconds(600)), NOW).size());
        assertEquals(1, EventRules.check(event(EventType.PAGE_VIEW, null, null, null, null, null, NOW.minusSeconds(8 * 86400L)), NOW).size());
    }

    @Test
    void moneyPrecisionAndEventIdAreValidated() {
        EventRequest tooPrecise = event(EventType.PURCHASE_COMPLETED, "p1", "books", 1, new BigDecimal("1.999"), "USD", NOW);
        assertTrue(V.validate(tooPrecise).stream().anyMatch(v -> v.getPropertyPath().toString().equals("amount")));
        EventRequest badId = new EventRequest("bad id!", EventType.PAGE_VIEW, NOW, "u1", "s1", null, null, null, null, null,
                DeviceType.MOBILE, "US-EAST", "web", 1);
        assertTrue(V.validate(badId).stream().anyMatch(v -> v.getPropertyPath().toString().equals("eventId")));
    }

    @Test
    void unsupportedSchemaVersionIsRejected() {
        EventRequest e = new EventRequest("evt-00000001", EventType.PAGE_VIEW, NOW, "u1", "s1", null, null, null, null, null,
                DeviceType.MOBILE, "US-EAST", "web", 2);
        assertEquals(1, EventRules.check(e, NOW).size());
    }
}
