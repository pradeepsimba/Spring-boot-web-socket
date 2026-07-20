package org.example.algosocket.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.findAndRegisterModules();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // FAIL_ON_UNKNOWN_PROPERTIES defaults to true - this mapper binds the one-shot query
        // path's FilterCriteria (HistoricalDataWebSocketHandler.handleTextMessage), so any client
        // that sends an extra field alongside the filter fields (a correlation id, a stray "type"
        // discriminator) would otherwise get a hard UnrecognizedPropertyException instead of the
        // field just being ignored.
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }
}
