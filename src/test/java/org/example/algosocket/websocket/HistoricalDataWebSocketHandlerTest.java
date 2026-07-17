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

        verify(matching, times(1)).sendMessage(any(TextMessage.class));
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

        verify(session, times(2)).sendMessage(any(TextMessage.class)); // one-shot reply + live broadcast
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

    private TextMessage liveFeedInit(String stockname, String symbol, String interval) throws Exception {
        String json = String.format(
                "{\"type\":\"LIVE_FEED_INIT\",\"filters\":[{\"stockname\":\"%s\",\"stock_symbol\":\"%s\",\"interval\":\"%s\"}]}",
                stockname, symbol, interval);
        return new TextMessage(json);
    }
}
