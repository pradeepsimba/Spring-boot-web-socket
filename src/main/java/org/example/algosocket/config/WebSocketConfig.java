package org.example.algosocket.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final HistoricalDataWebSocketHandler historicalDataWebSocketHandler;
    private final ObjectMapper objectMapper;

    public WebSocketConfig(HistoricalDataWebSocketHandler historicalDataWebSocketHandler, ObjectMapper objectMapper) {
        this.historicalDataWebSocketHandler = historicalDataWebSocketHandler;
        this.objectMapper = objectMapper;
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(historicalDataWebSocketHandler, "/historical-data").setAllowedOrigins("*");
    }
}
