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
        if (listener.isConnected()) {
            return Health.up().withDetail("liveFeedListener", "connected").build();
        }
        return Health.down().withDetail("liveFeedListener", "disconnected, reconnecting").build();
    }
}
