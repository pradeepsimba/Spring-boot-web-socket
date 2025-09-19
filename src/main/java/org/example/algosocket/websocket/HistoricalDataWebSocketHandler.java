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
    // Broadcast real-time data to matching sessions
    public void broadcastRealTimeData(org.example.algosocket.model.HistoricalData data) {
        liveFeedSessions.forEach(session -> {
            FilterCriteria filterCriteria = sessionFilters.get(session.getId());
            if (filterCriteria != null && filterCriteria.getFilterObjects() != null) {
                for (FilterCriteria.FilterObject fo : filterCriteria.getFilterObjects()) {
                    if (data.getStockname().equals(fo.getStockname()) &&
                        data.getStockSymbol().equals(fo.getStockSymbol()) &&
                        data.getInterval().equals(fo.getInterval())) {
                        try {
                            String json = objectMapper.writeValueAsString(java.util.Collections.singletonList(data));
                            session.sendMessage(new TextMessage(json));
                        } catch (Exception e) {
                            LOGGER.warning("Failed to send real-time data: " + e.getMessage());
                        }
                        break;
                    }
                }
            }
        });
    }

    private static final Logger LOGGER = Logger.getLogger(HistoricalDataWebSocketHandler.class.getName());

    private final HistoricalDataService historicalDataService;
    private final ObjectMapper objectMapper;

    private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final List<WebSocketSession> liveFeedSessions = new CopyOnWriteArrayList<>();
    private final Map<String, FilterCriteria> sessionFilters = new ConcurrentHashMap<>();

    public HistoricalDataWebSocketHandler(HistoricalDataService historicalDataService, ObjectMapper objectMapper) {
    this.historicalDataService = historicalDataService;
    this.objectMapper = objectMapper;
    // Real-time event streaming: scheduler removed
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        LOGGER.info("WebSocket connection established from: " + session.getRemoteAddress());
        sessions.add(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        LOGGER.info("Received message: " + message.getPayload());
        Map<String, Object> messageMap = objectMapper.readValue(message.getPayload(), Map.class);

        if ("LIVE_FEED_INIT".equals(messageMap.get("type"))) {
            liveFeedSessions.add(session);
            LOGGER.info("Live feed session initialized: " + session.getId());
            // Parse filters from frontend and set sessionFilters for live feed
            Object filtersObj = messageMap.get("filters");
            boolean latestOnly = Boolean.TRUE.equals(messageMap.get("latestOnly"));
            if (filtersObj instanceof List) {
                List<?> filters = (List<?>) filtersObj;
                List<FilterCriteria.FilterObject> filterObjects = new java.util.ArrayList<>();
                for (Object filter : filters) {
                    if (filter instanceof Map) {
                        Map<?,?> filterMap = (Map<?,?>) filter;
                        FilterCriteria.FilterObject fo = new FilterCriteria.FilterObject();
                        Object stockname = filterMap.get("stockname");
                        Object stockSymbol = filterMap.get("stock_symbol");
                        Object interval = filterMap.get("interval");
                        if (stockname != null) fo.setStockname(stockname.toString());
                        if (stockSymbol != null) fo.setStockSymbol(stockSymbol.toString());
                        if (interval != null) fo.setInterval(interval.toString());
                        filterObjects.add(fo);
                    }
                }
                FilterCriteria filterCriteria = new FilterCriteria();
                filterCriteria.setFilterObjects(filterObjects);
                sessionFilters.put(session.getId(), filterCriteria);
                session.getAttributes().put("latestOnly", latestOnly);
            }
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
    // scheduleUpdates removed for real-time event streaming
    }
}
