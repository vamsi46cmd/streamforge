package io.streamforge.flink;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

final class Json {
    /** Decimals are parsed as BigDecimal (never double) and written as plain numbers (never 1E+3). */
    static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private Json() {}
}
