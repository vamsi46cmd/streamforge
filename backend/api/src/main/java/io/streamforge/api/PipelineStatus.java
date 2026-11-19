package io.streamforge.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/** Live status of Kafka and Flink. Every method returns "unknown" (empty/null) rather than a fake zero. */
@Component
public class PipelineStatus {
    static final String JOB_NAME = "StreamForge event analytics";
    private final Admin admin;
    private final Config.Props props;
    private final ObjectMapper mapper;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public PipelineStatus(Admin admin, Config.Props props, ObjectMapper mapper) {
        this.admin = admin;
        this.props = props;
        this.mapper = mapper;
    }

    public boolean kafkaReachable() {
        try {
            return !admin.describeCluster().nodes().get(2, TimeUnit.SECONDS).isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    /** Sum over partitions of (log end offset - Flink's committed offset). Empty = not measurable right now. */
    public OptionalLong consumerLag() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(props.kafka().flinkGroup())
                    .partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);
            if (committed.isEmpty()) {
                return OptionalLong.empty(); // Flink has not committed yet (job not started / first checkpoint pending)
            }
            Map<TopicPartition, OffsetSpec> latest = committed.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            var ends = admin.listOffsets(latest).all().get(3, TimeUnit.SECONDS);
            long lag = 0;
            for (var en : committed.entrySet()) {
                lag += Math.max(0, ends.get(en.getKey()).offset() - en.getValue().offset());
            }
            return OptionalLong.of(lag);
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    /** State of the StreamForge Flink job (e.g. RUNNING, RESTARTING), or null if Flink cannot be reached. */
    public String flinkJobState() {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(props.flink().restUrl() + "/jobs/overview"))
                    .timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                return null;
            }
            JsonNode jobs = mapper.readTree(res.body()).get("jobs");
            String state = "NOT_SUBMITTED";
            if (jobs != null) {
                for (JsonNode j : jobs) {
                    if (JOB_NAME.equals(j.path("name").asText())) {
                        String s = j.path("state").asText();
                        if ("RUNNING".equals(s)) {
                            return s;
                        }
                        state = s;
                    }
                }
            }
            return state;
        } catch (Exception e) {
            return null;
        }
    }
}
