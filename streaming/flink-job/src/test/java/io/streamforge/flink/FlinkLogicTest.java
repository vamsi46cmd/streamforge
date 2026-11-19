package io.streamforge.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class FlinkLogicTest {
    private static final Instant NOW = Instant.parse("2026-11-18T12:00:00Z");

    private static String json(String type, String extra) {
        return "{\"eventId\":\"evt-00000001\",\"eventType\":\"" + type + "\",\"eventTimestamp\":\"2026-11-18T11:59:30Z\","
                + "\"userId\":\"u1\",\"sessionId\":\"s1\",\"deviceType\":\"MOBILE\",\"region\":\"US-EAST\","
                + "\"source\":\"web\",\"schemaVersion\":1" + extra + "}";
    }

    @Test
    void parsesValidPurchaseWithExactDecimal() {
        Event e = EventParser.parse(json("PURCHASE_COMPLETED",
                ",\"productId\":\"p1\",\"category\":\"books\",\"quantity\":2,\"amount\":19.90,\"currency\":\"USD\""), NOW);
        assertEquals(new BigDecimal("19.90"), e.amount);
        assertEquals(Instant.parse("2026-11-18T11:59:30Z").toEpochMilli(), e.eventTimeMs);
    }

    @Test
    void rejectsPurchaseWithoutAmount() {
        assertThrows(IllegalArgumentException.class, () -> EventParser.parse(
                json("PURCHASE_COMPLETED", ",\"productId\":\"p1\",\"category\":\"books\",\"quantity\":1,\"currency\":\"USD\""), NOW));
    }

    @Test
    void rejectsMalformedJsonUnknownTypeBadCurrencyAndFutureTimestamp() {
        assertThrows(IllegalArgumentException.class, () -> EventParser.parse("{not json", NOW));
        assertThrows(IllegalArgumentException.class, () -> EventParser.parse(json("NOPE", ""), NOW));
        assertThrows(IllegalArgumentException.class, () -> EventParser.parse(
                json("PAGE_VIEW", ",\"currency\":\"EUR\""), NOW));
        assertThrows(IllegalArgumentException.class, () -> EventParser.parse(
                json("PAGE_VIEW", "").replace("11:59:30", "13:59:30"), NOW));
    }

    @Test
    void rejectsAmountWithMoreThanTwoDecimals() {
        assertThrows(IllegalArgumentException.class, () -> EventParser.parse(
                json("PURCHASE_COMPLETED",
                        ",\"productId\":\"p1\",\"category\":\"books\",\"quantity\":1,\"amount\":1.999,\"currency\":\"USD\""), NOW));
    }

    private static Event ev(String type, String user, String category, String amount) {
        Event e = new Event();
        e.eventId = type + user + category + amount;
        e.eventType = type;
        e.userId = user;
        e.category = category;
        e.productId = category == null ? null : "p-" + category;
        e.amount = amount == null ? null : new BigDecimal(amount);
        e.region = "US-EAST";
        e.deviceType = "MOBILE";
        return e;
    }

    @Test
    void aggregatesCountsRevenueAndExactDistinctUsers() {
        List<Event> events = List.of(
                ev("PAGE_VIEW", "u1", null, null),
                ev("PRODUCT_VIEW", "u1", "books", null),
                ev("CHECKOUT_STARTED", "u2", null, null),
                ev("PURCHASE_COMPLETED", "u2", "books", "10.10"),
                ev("PURCHASE_COMPLETED", "u3", "toys", "0.20"),
                ev("PURCHASE_FAILED", "u3", null, null));
        WindowResult r = Aggregation.aggregate(0L, events, true);
        assertEquals(6, r.totalEvents);
        assertEquals(1, r.pageViews);
        assertEquals(1, r.productViews);
        assertEquals(1, r.checkouts);
        assertEquals(2, r.purchases);
        assertEquals(1, r.failedPurchases);
        assertEquals(3, r.activeUsers);
        assertEquals(new BigDecimal("10.30"), r.revenue); // exact decimal arithmetic, no float drift
        assertTrue(r.watermarkPassed);
        WindowResult.DimRow books = java.util.Arrays.stream(r.dims)
                .filter(d -> d.dimension.equals("category") && d.key.equals("books")).findFirst().orElseThrow();
        assertEquals(2, books.events);
        assertEquals(new BigDecimal("10.10"), books.revenue);
    }

    @Test
    void revisionsAreStrictlyIncreasing() {
        long a = Aggregation.nextRevision();
        long b = Aggregation.nextRevision();
        assertTrue(b > a);
    }
}
