package io.streamforge.flink;

import java.io.Serializable;
import java.math.BigDecimal;

/** One firing of a one-minute event-time window. Later firings of the same window supersede earlier ones. */
public class WindowResult implements Serializable {
    public long windowStartMs;
    public long totalEvents;
    public long pageViews;
    public long productViews;
    public long addToCart;
    public long checkouts;
    public long purchases;
    public long failedPurchases;
    public BigDecimal revenue = BigDecimal.ZERO;
    /** Exact distinct userIds seen in this window. Not additive across windows. */
    public long activeUsers;
    /** True once the watermark passed the window end (on-time data complete; late events may still revise it). */
    public boolean watermarkPassed;
    public long revision;
    public DimRow[] dims = new DimRow[0];

    public WindowResult() {}

    public static class DimRow implements Serializable {
        public String dimension;
        public String key;
        public long events;
        public BigDecimal revenue = BigDecimal.ZERO;

        public DimRow() {}

        public DimRow(String dimension, String key, long events, BigDecimal revenue) {
            this.dimension = dimension;
            this.key = key;
            this.events = events;
            this.revenue = revenue;
        }
    }
}
