package io.streamforge.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Event contract v1. Field-level rules live here; cross-field rules are in {@link EventRules}.
 * Identifiers: 8-64 chars of [A-Za-z0-9_-] for eventId, up to 64 chars for the rest.
 * Timestamps: ISO-8601 UTC instants (e.g. 2026-11-18T12:00:00Z).
 * Money: positive decimal, at most 2 fraction digits, USD only; never a float.
 */
public record EventRequest(
        @NotBlank @Pattern(regexp = "^[A-Za-z0-9_-]{8,64}$", message = "must be 8-64 chars of [A-Za-z0-9_-]") String eventId,
        @NotNull EventType eventType,
        @NotNull Instant eventTimestamp,
        @NotBlank @Size(max = 64) String userId,
        @NotBlank @Size(max = 64) String sessionId,
        @Size(max = 64) String productId,
        @Size(max = 64) String category,
        @Min(1) @Max(100) Integer quantity,
        @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal amount,
        @Pattern(regexp = "^[A-Z]{3}$", message = "must be a 3-letter ISO code") String currency,
        @NotNull DeviceType deviceType,
        @NotBlank @Size(max = 32) String region,
        @NotBlank @Size(max = 32) String source,
        @NotNull Integer schemaVersion) {

    public enum EventType {
        PAGE_VIEW, PRODUCT_VIEW, ADD_TO_CART, CHECKOUT_STARTED, PURCHASE_COMPLETED, PURCHASE_FAILED
    }

    public enum DeviceType {
        DESKTOP, MOBILE, TABLET
    }
}
