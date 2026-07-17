package org.example.algosocket.config;

import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private static final int MAX_TEXT_MESSAGE_SIZE_BYTES = 64 * 1024;

    private final HistoricalDataWebSocketHandler historicalDataWebSocketHandler;
    private final String[] allowedOrigins;

    public WebSocketConfig(HistoricalDataWebSocketHandler historicalDataWebSocketHandler,
                            @Value("${websocket.allowed-origins:*}") String allowedOrigins) {
        this.historicalDataWebSocketHandler = historicalDataWebSocketHandler;
        this.allowedOrigins = allowedOrigins.split("\\s*,\\s*");
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(historicalDataWebSocketHandler, "/historical-data").setAllowedOrigins(allowedOrigins);
    }

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(MAX_TEXT_MESSAGE_SIZE_BYTES);
        container.setMaxBinaryMessageBufferSize(MAX_TEXT_MESSAGE_SIZE_BYTES);
        return container;
    }
}
