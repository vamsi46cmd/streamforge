package io.streamforge.flink;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * StreamForge job: events.raw.v1 -> parse/validate -> dedup -> event-time 1-minute windows -> ClickHouse.
 * Invalid and late records also go to events.dlq.v1.
 */
public class StreamForgeJob {
    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? d : v;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(Integer.parseInt(env("FLINK_PARALLELISM", "2")));
        senv.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        senv.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        senv.getCheckpointConfig().setCheckpointTimeout(60_000);
        senv.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.seconds(5)));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("events.raw.v1")
                .setGroupId("streamforge-flink")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = senv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-events");

        SingleOutputStreamOperator<Event> parsed = raw.process(new Operators.ParseFn()).name("parse-validate");
        SingleOutputStreamOperator<Event> deduped = parsed
                .keyBy(e -> e.eventId, Types.STRING)
                .process(new Operators.DedupFn()).name("dedup-by-eventId");

        SingleOutputStreamOperator<WindowResult> windows = deduped
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.eventTimeMs)
                        .withIdleness(Duration.ofSeconds(15)))
                .keyBy(e -> 0, Types.INT)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .trigger(new Operators.ProvisionalTrigger(5_000))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(2))
                .sideOutputLateData(Operators.LATE)
                .process(new Operators.MinuteWindowFunction()).name("minute-window-aggregate");

        windows.addSink(new ClickHouseSinks.WindowSink()).name("clickhouse-window-metrics");

        DataStream<Rejected> late = windows.getSideOutput(Operators.LATE)
                .map(e -> new Rejected("LATE", "after allowed lateness", e.eventId, null))
                .returns(Types.POJO(Rejected.class)).name("late-events");
        DataStream<Rejected> invalid = parsed.getSideOutput(Operators.INVALID);
        DataStream<Rejected> duplicates = deduped.getSideOutput(Operators.DUPLICATE);

        DataStream<Rejected> rejected = invalid.union(duplicates, late);
        rejected.addSink(new ClickHouseSinks.RejectedSink()).name("clickhouse-rejected");

        KafkaSink<String> dlq = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("events.dlq.v1")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        invalid.union(late)
                .map(Rejected::toDlqJson).returns(Types.STRING).name("to-dlq-json")
                .sinkTo(dlq).name("kafka-dlq");

        senv.execute("StreamForge event analytics");
    }
}
