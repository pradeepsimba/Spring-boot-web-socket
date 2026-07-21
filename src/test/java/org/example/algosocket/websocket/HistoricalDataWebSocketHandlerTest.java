package org.example.algosocket.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.service.HistoricalDataService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.net.InetSocketAddress;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HistoricalDataWebSocketHandlerTest {

    private HistoricalDataService historicalDataService;
    private HistoricalDataWebSocketHandler handler;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        // Use the REAL production mapper (snake_case naming strategy from JacksonConfig), not a
        // default ObjectMapper - otherwise these tests validate a camelCase wire format that no
        // real client actually sends, and pass even if production field binding is broken.
        objectMapper = new org.example.algosocket.config.JacksonConfig().objectMapper();
        historicalDataService = mock(HistoricalDataService.class);
        handler = new HistoricalDataWebSocketHandler(historicalDataService, objectMapper);
    }

    private WebSocketSession mockSession(String id) throws Exception {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn(id);
        when(session.getRemoteAddress()).thenReturn(new InetSocketAddress(0));
        return session;
    }

    private HistoricalData sampleData(String stockname, String symbol, String interval) {
        return new HistoricalData(stockname, symbol, interval, LocalDateTime.now(),
                100.0, 110.0, 90.0, 105.0, 1000L, LocalDateTime.now(), null, null, null);
    }

    @Test
    void broadcast_sendsOnlyToSessionsWithMatchingFilter() throws Exception {
        WebSocketSession matching = mockSession("s1");
        WebSocketSession nonMatching = mockSession("s2");

        handler.afterConnectionEstablished(matching);
        handler.handleTextMessage(matching, liveFeedInit("NIFTY 50", "NIFTY", "1m"));

        handler.afterConnectionEstablished(nonMatching);
        handler.handleTextMessage(nonMatching, liveFeedInit("TCS", "TCS", "5m"));

        handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));

        // Broadcasts are dispatched to a background executor now (see submitSend) rather than sent
        // synchronously on this thread - timeout() polls instead of asserting instantly, since the
        // send may not have run yet the moment broadcastRealTimeData() returns.
        verify(matching, timeout(1000).times(1)).sendMessage(any(TextMessage.class));
        verify(nonMatching, never()).sendMessage(any(TextMessage.class));
    }

    @Test
    void afterConnectionClosed_removesSessionFromBroadcast() throws Exception {
        WebSocketSession session = mockSession("s1");
        handler.afterConnectionEstablished(session);
        handler.handleTextMessage(session, liveFeedInit("NIFTY 50", "NIFTY", "1m"));

        handler.afterConnectionClosed(session, CloseStatus.NORMAL);
        handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));

        verify(session, never()).sendMessage(any(TextMessage.class));
    }

    @Test
    void oneShotQueryAfterLiveFeedInit_doesNotClobberLiveFeedSubscription() throws Exception {
        // Regression test: a client can send LIVE_FEED_INIT and later a plain filtered query on the
        // same socket. The one-shot query must not silently kill the live-feed subscription.
        WebSocketSession session = mockSession("s1");
        when(historicalDataService.getHistoricalDataAsJson(any())).thenReturn("[]");

        handler.afterConnectionEstablished(session);
        handler.handleTextMessage(session, liveFeedInit("NIFTY 50", "NIFTY", "1m"));

        // snake_case: the real wire format under the production mapper's naming strategy
        String oneShotQuery = "{\"stock_names\":[\"NIFTY 50\"]}";
        handler.handleTextMessage(session, new TextMessage(oneShotQuery));

        handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));

        // Assert the criteria actually bound from the snake_case payload - a wildcard-only stub
        // would pass even if field-name mapping were completely broken.
        ArgumentCaptor<org.example.algosocket.model.FilterCriteria> criteriaCaptor =
                ArgumentCaptor.forClass(org.example.algosocket.model.FilterCriteria.class);
        verify(historicalDataService).getHistoricalDataAsJson(criteriaCaptor.capture());
        assertThat(criteriaCaptor.getValue().getStockNames()).containsExactly("NIFTY 50");

        // one-shot reply (synchronous) + live broadcast (dispatched async - see submitSend)
        verify(session, timeout(1000).times(2)).sendMessage(any(TextMessage.class));
    }

    @Test
    void malformedMessage_sendsErrorInsteadOfThrowing() throws Exception {
        WebSocketSession session = mockSession("s1");
        handler.afterConnectionEstablished(session);

        handler.handleTextMessage(session, new TextMessage("not-json"));

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("error");
    }

    @Test
    void liveFeedInitWithLatestOnly_sendsBootstrapSnapshot() throws Exception {
        WebSocketSession session = mockSession("s1");
        when(historicalDataService.getLatestPerFilterAsJson(any())).thenReturn("[{\"stockname\":\"NIFTY 50\"}]");

        handler.afterConnectionEstablished(session);
        String json = "{\"type\":\"LIVE_FEED_INIT\",\"filters\":[{\"stockname\":\"NIFTY 50\",\"stock_symbol\":\"NIFTY\",\"interval\":\"1m\"}],\"latestOnly\":true}";
        handler.handleTextMessage(session, new TextMessage(json));

        verify(historicalDataService).getLatestPerFilterAsJson(any());
        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("NIFTY 50");
    }

    @Test
    void liveFeedInitWithMissingFilters_sendsErrorInsteadOfSilentNoOp() throws Exception {
        WebSocketSession session = mockSession("s1");
        handler.afterConnectionEstablished(session);

        handler.handleTextMessage(session, new TextMessage("{\"type\":\"LIVE_FEED_INIT\"}"));

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("error");
    }

    @Test
    void liveFeedInitWithNoUsableFilters_sendsErrorAndDoesNotRegister() throws Exception {
        WebSocketSession session = mockSession("s1");
        handler.afterConnectionEstablished(session);

        // Non-empty 'filters' list, but nothing usable in it.
        handler.handleTextMessage(session, new TextMessage("{\"type\":\"LIVE_FEED_INIT\",\"filters\":[123,\"x\"]}"));

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("error");

        // A subsequent broadcast must not reach this session - it was never validly registered.
        handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));
        verify(session, times(1)).sendMessage(any(TextMessage.class)); // only the error, no broadcast
    }

    @Test
    void liveFeedInitWithLatestOnly_sessionNotBroadcastEligibleUntilAfterSnapshot() throws Exception {
        // Real ordering test: the snapshot stub fires a live broadcast for a MATCHING candle while
        // the snapshot query is "running". With the correct snapshot-then-subscribe order, the
        // session isn't registered yet, so that broadcast reaches nobody - the session receives
        // ONLY the snapshot (1 message). With the buggy register-then-snapshot order, the broadcast
        // would also land, delivering a newer candle before the older snapshot (2 messages) - the
        // exact stale-overwrite race. Asserting exactly 1 send during init distinguishes them.
        WebSocketSession session = mockSession("s1");
        handler.afterConnectionEstablished(session);
        when(historicalDataService.getLatestPerFilterAsJson(any())).thenAnswer(inv -> {
            handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));
            return "[{\"stockname\":\"BOOT\"}]";
        });

        String json = "{\"type\":\"LIVE_FEED_INIT\",\"filters\":[{\"stockname\":\"NIFTY 50\",\"stock_symbol\":\"NIFTY\",\"interval\":\"1m\"}],\"latestOnly\":true}";
        handler.handleTextMessage(session, new TextMessage(json));

        // Only the snapshot; the concurrent broadcast must NOT have reached this session. Not
        // racy despite async dispatch: the session isn't in liveFeedIndex yet at this point, so
        // broadcastRealTimeData's lookup finds nothing to schedule at all - there's no pending
        // async task to wait for here, so an immediate verify is correct.
        verify(session, times(1)).sendMessage(any(TextMessage.class));

        // And after init completes, the session IS subscribed - a later broadcast reaches it,
        // dispatched async (see submitSend) - timeout() polls for it.
        handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));
        verify(session, timeout(1000).times(2)).sendMessage(any(TextMessage.class));
    }

    @Test
    void repeatedLiveFeedInit_doesNotDeliverDuplicateBroadcasts() throws Exception {
        // A client re-sending LIVE_FEED_INIT on the same socket must not get every tick twice.
        WebSocketSession session = mockSession("s1");
        handler.afterConnectionEstablished(session);
        handler.handleTextMessage(session, liveFeedInit("NIFTY 50", "NIFTY", "1m"));
        handler.handleTextMessage(session, liveFeedInit("NIFTY 50", "NIFTY", "1m"));

        handler.broadcastRealTimeData(sampleData("NIFTY 50", "NIFTY", "1m"));

        // Exactly one delivery for the single broadcast, despite two LIVE_FEED_INIT calls.
        // Dispatched async (see submitSend) - timeout() polls for it.
        verify(session, timeout(1000).times(1)).sendMessage(any(TextMessage.class));
    }

    private TextMessage liveFeedInit(String stockname, String symbol, String interval) throws Exception {
        String json = String.format(
                "{\"type\":\"LIVE_FEED_INIT\",\"filters\":[{\"stockname\":\"%s\",\"stock_symbol\":\"%s\",\"interval\":\"%s\"}]}",
                stockname, symbol, interval);
        return new TextMessage(json);
    }
}
