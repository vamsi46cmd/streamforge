package io.streamforge.api;

import jakarta.annotation.PreDestroy;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class Config implements WebMvcConfigurer {
    private Admin admin;

    @ConfigurationProperties("streamforge")
    public record Props(Topics topics, Kafka kafka, ClickHouse clickhouse, Flink flink, Cors cors) {
        public record Topics(String raw) {}
        public record Kafka(String flinkGroup, long publishTimeoutMs) {}
        public record ClickHouse(String url, String user, String password) {}
        public record Flink(String restUrl) {}
        public record Cors(List<String> allowedOrigins) {}
    }

    private final Props props;

    public Config(Props props) {
        this.props = props;
    }

    /** CORS is explicit: only the listed origins, only the methods this API serves. Development defaults only. */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(props.cors().allowedOrigins().toArray(String[]::new))
                .allowedMethods("GET", "POST")
                .allowedHeaders("Content-Type", "X-Correlation-Id")
                .maxAge(600);
    }

    @Bean
    Admin kafkaAdminClient(@Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000));
        return admin;
    }

    @PreDestroy
    void close() {
        if (admin != null) {
            admin.close();
        }
    }

    /** Carries X-Correlation-Id (or a generated one) into the log MDC and back in the response. */
    @Bean
    OncePerRequestFilter correlationFilter() {
        return new OncePerRequestFilter() {
            @Override
            protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
                    throws ServletException, IOException {
                String id = req.getHeader("X-Correlation-Id");
                if (id == null || !id.matches("[A-Za-z0-9_-]{1,64}")) {
                    id = UUID.randomUUID().toString();
                }
                MDC.put("correlationId", id);
                res.setHeader("X-Correlation-Id", id);
                try {
                    chain.doFilter(req, res);
                } finally {
                    MDC.remove("correlationId");
                }
            }
        };
    }
}
