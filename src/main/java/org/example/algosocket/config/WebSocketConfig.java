package org.example.algosocket.config;

import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final HistoricalDataWebSocketHandler historicalDataWebSocketHandler;

    public WebSocketConfig(HistoricalDataWebSocketHandler historicalDataWebSocketHandler) {
        this.historicalDataWebSocketHandler = historicalDataWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(historicalDataWebSocketHandler, "/historical-data").setAllowedOrigins("*");
    }
}
