package org.example.algosocket.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.service.HistoricalDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class HistoricalDataWebSocketHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalDataWebSocketHandler.class);

    /** Bounds the per-session filter list so a misbehaving client can't inflate broadcast scan cost. */
    private static final int MAX_FILTER_OBJECTS = 50;

    /** Guards a session from a slow/stalled consumer: max time a send may block, and max buffered bytes. */
    private static final int SEND_TIME_LIMIT_MS = 10_000;
    private static final int SEND_BUFFER_SIZE_LIMIT_BYTES = 512 * 1024;

    private final HistoricalDataService historicalDataService;
    private final ObjectMapper objectMapper;

    private final List<WebSocketSession> liveFeedSessions = new CopyOnWriteArrayList<>();
    // Live-feed subscription filters only. Kept separate from one-shot query criteria so a later
    // one-shot query on the same socket can't silently clobber (and thereby kill) the live feed.
    private final Map<String, FilterCriteria> liveFeedFilters = new ConcurrentHashMap<>();
    // Every send for a given session goes through the SAME decorator instance so concurrent sends
    // from the broadcast thread and the WebSocket container's own dispatch thread are serialized.
    private final Map<String, ConcurrentWebSocketSessionDecorator> sessionSenders = new ConcurrentHashMap<>();

    public HistoricalDataWebSocketHandler(HistoricalDataService historicalDataService, ObjectMapper objectMapper) {
        this.historicalDataService = historicalDataService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        LOGGER.info("WebSocket connection established from: {}", session.getRemoteAddress());
        sessionSenders.put(session.getId(),
                new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, SEND_BUFFER_SIZE_LIMIT_BYTES));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        LOGGER.debug("Received message: {}", message.getPayload());
        try {
            Map<String, Object> messageMap = objectMapper.readValue(message.getPayload(), new TypeReference<>() {});

            if ("LIVE_FEED_INIT".equals(messageMap.get("type"))) {
                initLiveFeed(session, messageMap);
            } else {
                FilterCriteria filterCriteria = objectMapper.readValue(message.getPayload(), FilterCriteria.class);
                String historicalDataJson = historicalDataService.getHistoricalDataAsJson(filterCriteria);
                send(session, historicalDataJson);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to process message from session {}: {}", session.getId(), e.getMessage());
            send(session, "{\"error\":\"invalid_request\"}");
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        LOGGER.info("WebSocket connection closed from: {}", session.getRemoteAddress());
        evict(session);
    }

    public void broadcastRealTimeData(HistoricalData data) {
        String json = null;
        for (WebSocketSession session : liveFeedSessions) {
            FilterCriteria filterCriteria = liveFeedFilters.get(session.getId());
            if (filterCriteria == null || filterCriteria.getFilterObjects() == null) continue;

            for (FilterCriteria.FilterObject fo : filterCriteria.getFilterObjects()) {
                if (matches(data, fo)) {
                    if (json == null) {
                        json = serializeSingleton(data);
                        if (json == null) return; // serialization failed; already logged
                    }
                    send(session, json);
                    break;
                }
            }
        }
    }

    private String serializeSingleton(HistoricalData data) {
        try {
            return objectMapper.writeValueAsString(List.of(data));
        } catch (Exception e) {
            LOGGER.warn("Failed to serialize real-time data: {}", e.getMessage());
            return null;
        }
    }

    private void send(WebSocketSession session, String payload) {
        ConcurrentWebSocketSessionDecorator sender = sessionSenders.get(session.getId());
        if (sender == null) return; // session already evicted
        try {
            sender.sendMessage(new TextMessage(payload));
        } catch (Exception e) {
            LOGGER.warn("Failed to send message to session {}, evicting: {}", session.getId(), e.getMessage());
            evict(session);
        }
    }

    private void evict(WebSocketSession session) {
        liveFeedFilters.remove(session.getId());
        liveFeedSessions.remove(session);
        sessionSenders.remove(session.getId());
    }

    private void initLiveFeed(WebSocketSession session, Map<String, Object> messageMap) {
        LOGGER.info("Live feed session initialized: {}", session.getId());

        Object filtersObj = messageMap.get("filters");
        if (!(filtersObj instanceof List<?> filters)) {
            // Let the caller's catch-all send {"error":"invalid_request"} instead of silently
            // registering an empty-filter session that will never receive anything.
            throw new IllegalArgumentException("LIVE_FEED_INIT requires a non-empty 'filters' list");
        }

        List<FilterCriteria.FilterObject> filterObjects = new ArrayList<>();
        for (Object filter : filters) {
            if (filterObjects.size() >= MAX_FILTER_OBJECTS) {
                LOGGER.warn("Session {} sent more than {} filters; truncating.", session.getId(), MAX_FILTER_OBJECTS);
                break;
            }
            if (filter instanceof Map<?, ?> filterMap) {
                FilterCriteria.FilterObject fo = new FilterCriteria.FilterObject();
                if (filterMap.get("stockname") != null) fo.setStockname(filterMap.get("stockname").toString());
                if (filterMap.get("stock_symbol") != null) fo.setStockSymbol(filterMap.get("stock_symbol").toString());
                if (filterMap.get("interval") != null) fo.setInterval(filterMap.get("interval").toString());
                filterObjects.add(fo);
            }
        }

        if (filterObjects.isEmpty()) {
            // Non-empty 'filters' list but nothing usable in it (non-map entries, or all fields
            // blank). Don't register a session that could never match anything - surface an error.
            throw new IllegalArgumentException("LIVE_FEED_INIT 'filters' contained no valid filter objects");
        }

        // Send the bootstrap snapshot BEFORE registering the session for live broadcasts. If we
        // registered first, a live tick (fired from the PG-listener thread) could interleave with
        // this snapshot and the client could receive a newer candle followed by the older snapshot,
        // overwriting fresh data with stale. Snapshot-then-subscribe guarantees the client never
        // sees a bootstrap value that's older than a live update it already applied.
        if (Boolean.TRUE.equals(messageMap.get("latestOnly"))) {
            try {
                send(session, historicalDataService.getLatestPerFilterAsJson(filterObjects));
            } catch (Exception e) {
                LOGGER.warn("Failed to send live-feed bootstrap snapshot to session {}: {}",
                        session.getId(), e.getMessage());
            }
        }

        FilterCriteria filterCriteria = new FilterCriteria();
        filterCriteria.setFilterObjects(filterObjects);
        // put() overwrites the filter idempotently, but liveFeedSessions is a CopyOnWriteArrayList
        // that allows duplicates - a client re-sending LIVE_FEED_INIT on the same socket would
        // otherwise appear twice and receive every matching tick twice. Guard against that.
        liveFeedFilters.put(session.getId(), filterCriteria);
        if (!liveFeedSessions.contains(session)) {
            liveFeedSessions.add(session);
        }
    }

    private boolean matches(HistoricalData data, FilterCriteria.FilterObject fo) {
        // Columns are nullable in the schema; an NPE here would abort the whole broadcast loop
        // (see broadcastRealTimeData), silently starving every session later in iteration order.
        return java.util.Objects.equals(data.getStockname(), fo.getStockname())
                && java.util.Objects.equals(data.getStockSymbol(), fo.getStockSymbol())
                && java.util.Objects.equals(data.getInterval(), fo.getInterval());
    }
}
