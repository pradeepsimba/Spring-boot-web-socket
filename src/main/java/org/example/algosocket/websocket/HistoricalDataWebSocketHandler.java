package org.example.algosocket.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.service.HistoricalDataService;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

@Component
public class HistoricalDataWebSocketHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = Logger.getLogger(HistoricalDataWebSocketHandler.class.getName());

    private final HistoricalDataService historicalDataService;
    private final ObjectMapper objectMapper;

    private final List<WebSocketSession> liveFeedSessions = new CopyOnWriteArrayList<>();
    private final Map<String, FilterCriteria> sessionFilters = new ConcurrentHashMap<>();

    public HistoricalDataWebSocketHandler(HistoricalDataService historicalDataService, ObjectMapper objectMapper) {
        this.historicalDataService = historicalDataService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        LOGGER.info("WebSocket connection established from: " + session.getRemoteAddress());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        LOGGER.info("Received message: " + message.getPayload());
        Map<String, Object> messageMap = objectMapper.readValue(message.getPayload(), new TypeReference<>() {});

        if ("LIVE_FEED_INIT".equals(messageMap.get("type"))) {
            initLiveFeed(session, messageMap);
        } else {
            FilterCriteria filterCriteria = objectMapper.readValue(message.getPayload(), FilterCriteria.class);
            sessionFilters.put(session.getId(), filterCriteria);
            String historicalDataJson = historicalDataService.getHistoricalDataAsJson(filterCriteria);
            session.sendMessage(new TextMessage(historicalDataJson));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        LOGGER.info("WebSocket connection closed from: " + session.getRemoteAddress());
        sessionFilters.remove(session.getId());
        liveFeedSessions.remove(session);
    }

    public void broadcastRealTimeData(HistoricalData data) {
        for (WebSocketSession session : liveFeedSessions) {
            FilterCriteria filterCriteria = sessionFilters.get(session.getId());
            if (filterCriteria == null || filterCriteria.getFilterObjects() == null) continue;

            for (FilterCriteria.FilterObject fo : filterCriteria.getFilterObjects()) {
                if (matches(data, fo)) {
                    try {
                        String json = objectMapper.writeValueAsString(Collections.singletonList(data));
                        session.sendMessage(new TextMessage(json));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to send real-time data: " + e.getMessage());
                    }
                    break;
                }
            }
        }
    }

    private void initLiveFeed(WebSocketSession session, Map<String, Object> messageMap) {
        liveFeedSessions.add(session);
        LOGGER.info("Live feed session initialized: " + session.getId());

        Object filtersObj = messageMap.get("filters");
        if (!(filtersObj instanceof List<?> filters)) return;

        List<FilterCriteria.FilterObject> filterObjects = new ArrayList<>();
        for (Object filter : filters) {
            if (filter instanceof Map<?, ?> filterMap) {
                FilterCriteria.FilterObject fo = new FilterCriteria.FilterObject();
                if (filterMap.get("stockname") != null) fo.setStockname(filterMap.get("stockname").toString());
                if (filterMap.get("stock_symbol") != null) fo.setStockSymbol(filterMap.get("stock_symbol").toString());
                if (filterMap.get("interval") != null) fo.setInterval(filterMap.get("interval").toString());
                filterObjects.add(fo);
            }
        }
        FilterCriteria filterCriteria = new FilterCriteria();
        filterCriteria.setFilterObjects(filterObjects);
        sessionFilters.put(session.getId(), filterCriteria);
    }

    private boolean matches(HistoricalData data, FilterCriteria.FilterObject fo) {
        return data.getStockname().equals(fo.getStockname())
                && data.getStockSymbol().equals(fo.getStockSymbol())
                && data.getInterval().equals(fo.getInterval());
    }
}
