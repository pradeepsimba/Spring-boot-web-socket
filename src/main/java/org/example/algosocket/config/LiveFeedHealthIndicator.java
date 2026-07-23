package org.example.algosocket.config;

import org.example.algosocket.service.PostgresNotificationListener;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/** Reports whether the live-feed Postgres LISTEN connection is currently up, not just whether the DB is reachable. */
@Component
public class LiveFeedHealthIndicator implements HealthIndicator {

    private final PostgresNotificationListener listener;

    public LiveFeedHealthIndicator(PostgresNotificationListener listener) {
        this.listener = listener;
    }

    @Override
    public Health health() {
        // Two independent failure modes, both against the live-feed pool: the permanent LISTEN
        // connection (isConnected) and the separate fetch-worker path (isFetchHealthy) that borrows
        // its own connections from the same pool. A NOTIFY batch that repeatedly fails to fetch
        // would previously leave this indicator reporting UP as long as the LISTEN connection alone
        // was fine, even though every client subscribed to the affected rows was silently getting
        // no updates at all.
        boolean listenerUp = listener.isConnected();
        boolean fetchUp    = listener.isFetchHealthy();
        Health.Builder builder = (listenerUp && fetchUp) ? Health.up() : Health.down();
        return builder
                .withDetail("liveFeedListener", listenerUp ? "connected" : "disconnected, reconnecting")
                .withDetail("liveFeedFetch", fetchUp ? "healthy" : "repeated fetch failures")
                .build();
    }
}
