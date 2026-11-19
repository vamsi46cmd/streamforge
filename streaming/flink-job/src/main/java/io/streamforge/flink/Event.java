package io.streamforge.flink;

import java.io.Serializable;
import java.math.BigDecimal;

/** Validated event. Public fields + default constructor make this a Flink POJO (efficient serializer). */
public class Event implements Serializable {
    public String eventId;
    public String eventType;
    public long eventTimeMs;
    public String userId;
    public String sessionId;
    public String productId;
    public String category;
    public Integer quantity;
    public BigDecimal amount;
    public String currency;
    public String deviceType;
    public String region;
    public String source;

    public Event() {}
}
