package io.streamforge.flink;

import java.time.Instant;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Stream operators for the StreamForge job. */
public final class Operators {
    public static final OutputTag<Rejected> INVALID = new OutputTag<>("invalid", Types.POJO(Rejected.class));
    public static final OutputTag<Rejected> DUPLICATE = new OutputTag<>("duplicate", Types.POJO(Rejected.class));
    public static final OutputTag<Event> LATE = new OutputTag<>("late", Types.POJO(Event.class));

    private Operators() {}

    /** Parses and validates; permanently invalid records go to the INVALID side output (never dropped silently). */
    public static class ParseFn extends ProcessFunction<String, Event> {
        @Override
        public void processElement(String value, Context ctx, Collector<Event> out) {
            try {
                out.collect(EventParser.parse(value, Instant.now()));
            } catch (IllegalArgumentException e) {
                ctx.output(INVALID, new Rejected("INVALID", e.getMessage(), EventParser.tryEventId(value), value));
            }
        }
    }

    /**
     * eventId-based deduplication with keyed state. State expires 1 hour after the first sighting, so a
     * duplicate arriving more than 1 hour after the original is NOT detected (documented limitation).
     */
    public static class DedupFn extends KeyedProcessFunction<String, Event, Event> {
        private transient ValueState<Boolean> seen;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> d = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
            d.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
            seen = getRuntimeContext().getState(d);
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<Event> out) throws Exception {
            if (seen.value() == null) {
                seen.update(true);
                out.collect(e);
            } else {
                ctx.output(DUPLICATE, new Rejected("DUPLICATE", "duplicate eventId", e.eventId, null));
            }
        }
    }

    /**
     * Fires when the watermark reaches the window end (on-time result), every {@code intervalMs} of processing
     * time while data keeps arriving (provisional results for the live dashboard), and on every element that
     * arrives after the window end but within allowed lateness (revised result). Never purges: the window
     * is re-computed from all its elements, so each firing fully supersedes the previous one.
     */
    public static class ProvisionalTrigger extends Trigger<Event, TimeWindow> {
        private final long intervalMs;
        private final ValueStateDescriptor<Boolean> armed = new ValueStateDescriptor<>("armed", Types.BOOLEAN);

        public ProvisionalTrigger(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        @Override
        public TriggerResult onElement(Event e, long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            if (w.maxTimestamp() <= ctx.getCurrentWatermark()) {
                return TriggerResult.FIRE; // late but within allowed lateness
            }
            ctx.registerEventTimeTimer(w.maxTimestamp());
            ValueState<Boolean> s = ctx.getPartitionedState(armed);
            if (s.value() == null) {
                s.update(true);
                ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + intervalMs);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow w, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(armed).clear();
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow w, TriggerContext ctx) {
            return time == w.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow w, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(w.maxTimestamp());
            ctx.getPartitionedState(armed).clear();
        }
    }

    /** Constant-key window (one global aggregate per minute); keyed so the context exposes the watermark. */
    public static class MinuteWindowFunction extends ProcessWindowFunction<Event, WindowResult, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context ctx, Iterable<Event> elements, Collector<WindowResult> out) {
            boolean passed = ctx.currentWatermark() >= ctx.window().maxTimestamp();
            out.collect(Aggregation.aggregate(ctx.window().getStart(), elements, passed));
        }
    }
}
