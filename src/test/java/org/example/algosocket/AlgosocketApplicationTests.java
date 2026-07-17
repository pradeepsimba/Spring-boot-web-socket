package org.example.algosocket;

import org.example.algosocket.repository.HistoricalDataRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * Smoke test for full application wiring. HistoricalDataRepository is mocked so the test doesn't
 * require a real Postgres instance. Uses a real embedded servlet container (RANDOM_PORT) because
 * WebSocketConfig registers a ServletServerContainerFactoryBean, which requires a genuine
 * jakarta.websocket ServerContainer attribute that a MOCK web environment does not provide.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AlgosocketApplicationTests {

    @MockBean
    private HistoricalDataRepository historicalDataRepository;

    @Test
    void contextLoads() {
    }
}
