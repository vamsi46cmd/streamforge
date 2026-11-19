package io.streamforge.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class StreamForgeApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamForgeApiApplication.class, args);
    }
}
