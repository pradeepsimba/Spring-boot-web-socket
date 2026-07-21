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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.annotation.PreDestroy;

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

    // Live-feed subscription filters only. Kept separate from one-shot query criteria so a later
    // one-shot query on the same socket can't silently clobber (and thereby kill) the live feed.
    private final Map<String, FilterCriteria> liveFeedFilters = new ConcurrentHashMap<>();
    // Every send for a given session goes through the SAME decorator instance so concurrent sends
    // from the broadcast thread and the WebSocket container's own dispatch thread are serialized.
    private final Map<String, ConcurrentWebSocketSessionDecorator> sessionSenders = new ConcurrentHashMap<>();
    // Reverse index: (stockname, stock_symbol, interval) -> sessions subscribed to exactly that
    // triple. broadcastRealTimeData used to scan every live session x every one of its filters
    // (O(sessions x filters) per tick) - with thousands of ticks/sec and dozens of sessions each
    // with dozens of filters, that scan dominated tick-handling cost. A tick now does one map
    // lookup instead.
    private final Map<FilterKey, CopyOnWriteArrayList<WebSocketSession>> liveFeedIndex = new ConcurrentHashMap<>();

    // broadcastRealTimeData runs on PostgresNotificationListener's single dedicated listener
    // thread. ConcurrentWebSocketSessionDecorator's SEND_TIME_LIMIT_MS only aborts a send that
    // OVERLAPS a second concurrent send to the SAME session - a lone blocking write (a dead TCP
    // peer that stopped ACKing: sleeping laptop, dropped Wi-Fi, a slow-reading client) has nothing
    // to race against and can block for however long the OS/TCP stack allows, often minutes. While
    // blocked, this was the ONE thread that drains Postgres NOTIFYs - every other live session,
    // regardless of filter, stopped receiving ticks for as long as that one client stayed stuck.
    // Sends are dispatched here instead, hash-partitioned by session id across a fixed pool of
    // single-thread bounded-queue executors (mirrors AngelOne_parallel_server's wsExecutors
    // sharding pattern) - EVERY session's sends always land on the SAME executor, so ordering
    // within one session's live feed is preserved, while different sessions run on different
    // threads so one stuck session can only ever block the (at most 1/BROADCAST_PARALLELISM)
    // share of sessions hashed to the same executor, never the listener thread itself.
    private static final int BROADCAST_PARALLELISM = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final int BROADCAST_QUEUE_CAPACITY = 5_000;
    private final AtomicLong droppedBroadcasts = new AtomicLong();
    private final ExecutorService[] broadcastExecutors = new ExecutorService[BROADCAST_PARALLELISM];
    {
        for (int i = 0; i < BROADCAST_PARALLELISM; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(BROADCAST_QUEUE_CAPACITY));
            executor.setRejectedExecutionHandler((task, exec) -> {
                long dropped = droppedBroadcasts.incrementAndGet();
                if (dropped % 1000 == 1) {
                    LOGGER.warn("Broadcast executor queue full; dropped {} sends so far.", dropped);
                }
            });
            broadcastExecutors[i] = executor;
        }
    }

    @PreDestroy
    public void shutdownBroadcastExecutors() {
        for (ExecutorService executor : broadcastExecutors) {
            executor.shutdown();
        }
    }

    private void submitSend(WebSocketSession session, String payload) {
        int idx = Math.floorMod(session.getId().hashCode(), BROADCAST_PARALLELISM);
        broadcastExecutors[idx].execute(() -> send(session, payload));
    }

    private record FilterKey(String stockname, String stockSymbol, String interval) {
        // stockname/stock_symbol normalized to uppercase on both sides of this key (client filter
        // AND broadcast data) so a client subscribing with different casing than what's actually
        // stored still matches - mirrors the Django backend's get_historical_data, which filters
        // these two case-insensitively (__iexact) and has a purpose-built expression index for it
        // (app_histori_upper_covering_idx). Without this, a client using the same mixed-case
        // values the Django API's own docstring example uses ("hdfc bank") would never receive any
        // live ticks for that filter, silently - no error, just permanent non-delivery. interval
        // stays as-is: it's a fixed lowercase token ("1m"/"5m"/"1d"), never user-typed with varying
        // case, matching Django's own exact (non-iexact) interval filter.
        private static String normalize(String s) {
            return s == null ? null : s.toUpperCase(java.util.Locale.ROOT);
        }
        static FilterKey of(FilterCriteria.FilterObject fo) {
            return new FilterKey(normalize(fo.getStockname()), normalize(fo.getStockSymbol()), fo.getInterval());
        }
        static FilterKey of(HistoricalData d) {
            return new FilterKey(normalize(d.getStockname()), normalize(d.getStockSymbol()), d.getInterval());
        }
    }

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
        List<WebSocketSession> sessions = liveFeedIndex.get(FilterKey.of(data));
        if (sessions == null || sessions.isEmpty()) return;

        String json = serializeSingleton(data);
        if (json == null) return; // serialization failed; already logged

        for (WebSocketSession session : sessions) {
            submitSend(session, json);
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
        // synchronized(session): evict() can run on the PG-listener thread (send() failing mid-
        // broadcast) at the same time initLiveFeed() runs on this session's own WS dispatch thread
        // (a client re-subscribing with new filters). Without a shared lock, evict()'s
        // removeFromIndex could run between initLiveFeed's liveFeedFilters.put() and its
        // addToIndex() - removeFromIndex would find nothing to remove (not indexed yet under the
        // new filter), sessionSenders.remove() would already have run, and then initLiveFeed's
        // addToIndex would still re-insert the now-dead session into liveFeedIndex with nothing
        // left to ever clean it up (no further message/close event fires on a dead socket).
        // WebSocketSession is stable for the connection's lifetime, so it's a safe lock object.
        synchronized (session) {
            FilterCriteria previous = liveFeedFilters.remove(session.getId());
            removeFromIndex(session, previous);
            sessionSenders.remove(session.getId());
        }
    }

    private void addToIndex(WebSocketSession session, List<FilterCriteria.FilterObject> filterObjects) {
        for (FilterCriteria.FilterObject fo : filterObjects) {
            liveFeedIndex.computeIfAbsent(FilterKey.of(fo), k -> new CopyOnWriteArrayList<>()).addIfAbsent(session);
        }
    }

    private void removeFromIndex(WebSocketSession session, FilterCriteria previous) {
        if (previous == null || previous.getFilterObjects() == null) return;
        for (FilterCriteria.FilterObject fo : previous.getFilterObjects()) {
            // computeIfPresent (not a plain get()+remove()) so removing the last session for a
            // FilterKey also drops the map entry itself - otherwise every distinct
            // (stockname, stockSymbol, interval) triple ever subscribed to leaves a permanent
            // (eventually empty) CopyOnWriteArrayList behind for the life of the process. A single
            // client repeatedly subscribing with different filter values then disconnecting is an
            // unbounded-memory growth path with no cap (MAX_FILTER_OBJECTS only bounds one
            // message's filter count, not the number of distinct filters ever seen).
            liveFeedIndex.computeIfPresent(FilterKey.of(fo), (k, sessions) -> {
                sessions.remove(session);
                return sessions.isEmpty() ? null : sessions;
            });
        }
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
        // synchronized(session), matching evict(): without it, an eviction triggered by an
        // unrelated broadcast's send() failure (on the PG-listener thread) could interleave with
        // this swap and get re-registered into liveFeedIndex a moment later with nothing left to
        // ever clean it up (see evict()'s comment). The sessionSenders check inside the lock closes
        // the remaining gap even for evictions that complete just BEFORE this block acquires the
        // lock (not just ones interleaved during it): if the session's already gone, don't
        // resurrect it - a message that arrived just as the connection died is already stale.
        synchronized (session) {
            if (!sessionSenders.containsKey(session.getId())) {
                return;
            }
            // A client re-sending LIVE_FEED_INIT on the same socket replaces its filters rather
            // than adding to them - remove the old index entries (using the filters actually
            // indexed under, not whatever the client sends this time) before indexing the new
            // ones, or a re-subscribe with a different filter set would leave the session
            // receiving ticks for both the old and new filters forever.
            FilterCriteria previous = liveFeedFilters.put(session.getId(), filterCriteria);
            removeFromIndex(session, previous);
            addToIndex(session, filterObjects);
        }
    }
}
