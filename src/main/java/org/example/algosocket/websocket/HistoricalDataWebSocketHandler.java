package org.example.algosocket.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.service.HistoricalDataService;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Component
public class HistoricalDataWebSocketHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = Logger.getLogger(HistoricalDataWebSocketHandler.class.getName());

    private final HistoricalDataService historicalDataService;
    private final ObjectMapper objectMapper;

    private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final List<WebSocketSession> liveFeedSessions = new CopyOnWriteArrayList<>();
    private final Map<String, FilterCriteria> sessionFilters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public HistoricalDataWebSocketHandler(HistoricalDataService historicalDataService, ObjectMapper objectMapper) {
        this.historicalDataService = historicalDataService;
        this.objectMapper = objectMapper;
        scheduleUpdates();
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        LOGGER.info("WebSocket connection established from: " + session.getRemoteAddress());
        sessions.add(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        LOGGER.info("Received message: " + message.getPayload());
        Map<String, String> messageMap = objectMapper.readValue(message.getPayload(), Map.class);

        if ("LIVE_FEED_INIT".equals(messageMap.get("type"))) {
            liveFeedSessions.add(session);
            LOGGER.info("Live feed session initialized: " + session.getId());
        } else {
            FilterCriteria filterCriteria = objectMapper.readValue(message.getPayload(), FilterCriteria.class);
            sessionFilters.put(session.getId(), filterCriteria);
            String historicalDataJson = historicalDataService.getHistoricalDataAsJson(filterCriteria);
            LOGGER.info("Sending data to client: " + historicalDataJson);
            session.sendMessage(new TextMessage(historicalDataJson));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        LOGGER.info("WebSocket connection closed from: " + session.getRemoteAddress());
        sessions.remove(session);
        sessionFilters.remove(session.getId());
        liveFeedSessions.remove(session);
    }

    private void scheduleUpdates() {
        scheduler.scheduleAtFixedRate(() -> {
            LOGGER.info("Scheduler is running...");
            // Send updates to historical data sessions based on their filters
            sessions.forEach(session -> {
                FilterCriteria filterCriteria = sessionFilters.get(session.getId());
                if (filterCriteria != null) {
                    String historicalDataJson = historicalDataService.getHistoricalDataAsJson(filterCriteria);
                    if (!"[]".equals(historicalDataJson)) {
                        LOGGER.info("Found new data for historical session: " + session.getId());
                        try {
                            session.sendMessage(new TextMessage(historicalDataJson));
                        } catch (IOException e) {
                            LOGGER.warning("Failed to send message to historical session: " + session.getId() + " " + e.getMessage());
                        }
                    }
                }
            });

            // Send latest updates to live feed sessions
            String latestData = historicalDataService.getLatestDataAsJson(new FilterCriteria()); // Get all latest data
            if (!"[]".equals(latestData)) {
                liveFeedSessions.forEach(session -> {
                    LOGGER.info("Found new data for live feed session: " + session.getId());
                    try {
                        session.sendMessage(new TextMessage(latestData));
                    } catch (IOException e) {
                        LOGGER.warning("Failed to send message to live feed session: " + session.getId() + " " + e.getMessage());
                    }
                });
            }

        }, 5, 5, TimeUnit.SECONDS);
    }
}
