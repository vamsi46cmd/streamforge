package io.streamforge.flink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;

/** A record the pipeline did not aggregate. kind = INVALID | DUPLICATE | LATE. */
public class Rejected implements Serializable {
    public String kind;
    public String reason;
    public String eventId;
    /** Original payload; only populated for INVALID records and only sent to the DLQ topic. */
    public String payload;
    public long tsMs;

    public Rejected() {}

    public Rejected(String kind, String reason, String eventId, String payload) {
        this.kind = kind;
        this.reason = reason;
        this.eventId = eventId == null ? "" : eventId;
        this.payload = payload;
        this.tsMs = System.currentTimeMillis();
    }

    public String toDlqJson() {
        ObjectNode n = Json.MAPPER.createObjectNode();
        n.put("kind", kind);
        n.put("reason", reason);
        n.put("eventId", eventId);
        if (payload != null) {
            n.put("payload", payload.length() > 2000 ? payload.substring(0, 2000) : payload);
        }
        return n.toString();
    }
}
