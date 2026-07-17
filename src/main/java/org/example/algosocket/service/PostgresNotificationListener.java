package org.example.algosocket.service;

import org.example.algosocket.model.HistoricalDataRowMapper;
import org.example.algosocket.repository.HistoricalDataQueryBuilder;
import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class PostgresNotificationListener implements Runnable, ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresNotificationListener.class);

    private static final String FETCH_BY_IDS_SQL_PREFIX =
            HistoricalDataQueryBuilder.BASE_SELECT + " WHERE h.id IN (";

    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final long NOTIFICATION_POLL_TIMEOUT_MS = 1_000;

    private final HistoricalDataWebSocketHandler webSocketHandler;
    private final DataSource dataSource;

    private Thread listenerThread;
    private volatile boolean running = true;
    private volatile boolean connected = false;
    private volatile boolean triggerConfirmed = false;

    public PostgresNotificationListener(HistoricalDataWebSocketHandler webSocketHandler, DataSource dataSource) {
        this.webSocketHandler = webSocketHandler;
        this.dataSource = dataSource;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        start();
    }

    public void start() {
        listenerThread = new Thread(this, "PG-Notification-Listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
    }

    /**
     * Reflects whether the listener currently holds a live LISTEN connection AND successfully
     * confirmed the NOTIFY trigger exists, for health checks. Being "connected" without the
     * trigger in place (e.g. the DB role lacks CREATE privilege) means notifications will never
     * arrive even though the listener looks healthy - surface that as DOWN, not UP.
     */
    public boolean isConnected() {
        return connected && triggerConfirmed;
    }

    @Override
    public void run() {
        long backoffMs = INITIAL_BACKOFF_MS;
        while (running) {
            try {
                listenOnce();
                backoffMs = INITIAL_BACKOFF_MS;
            } catch (Exception e) {
                LOGGER.warn("PostgresNotificationListener connection lost: {}", e.getMessage());
            } finally {
                connected = false;
                triggerConfirmed = false;
            }

            if (!running) break;

            LOGGER.info("Reconnecting to PostgreSQL notifications in {} ms...", backoffMs);
            if (!sleepInterruptibly(backoffMs)) break;
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
    }

    private void listenOnce() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            triggerConfirmed = createTriggerIfNeeded(conn);

            PGConnection pgConn = conn.unwrap(PGConnection.class);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN new_historical_data");
            }
            connected = true;
            LOGGER.info("Started listening for PostgreSQL notifications on 'new_historical_data'.");

            while (running) {
                PGNotification[] notifications = pgConn.getNotifications((int) NOTIFICATION_POLL_TIMEOUT_MS);
                if (notifications == null || notifications.length == 0) continue;

                List<Long> ids = new ArrayList<>(notifications.length);
                for (PGNotification notification : notifications) {
                    String payload = notification.getParameter();
                    LOGGER.debug("Received NOTIFY: {}", payload);
                    try {
                        ids.add(Long.parseLong(payload));
                    } catch (NumberFormatException e) {
                        LOGGER.warn("Ignoring non-numeric NOTIFY payload: {}", payload);
                    }
                }
                if (ids.isEmpty()) continue;

                fetchAndBroadcast(conn, ids);
            }
        }
    }

    private void fetchAndBroadcast(Connection conn, List<Long> ids) {
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = FETCH_BY_IDS_SQL_PREFIX + placeholders + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setLong(i + 1, ids.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    webSocketHandler.broadcastRealTimeData(HistoricalDataRowMapper.mapRow(rs));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error fetching/broadcasting new data for ids {}: {}", ids, e.getMessage());
        }
    }

    /**
     * @return true if the sleep completed normally, false if interrupted/shutting down.
     */
    private boolean sleepInterruptibly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** @return true if the trigger/function were confirmed present (created now or already existed). */
    private boolean createTriggerIfNeeded(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("""
                CREATE OR REPLACE FUNCTION notify_new_historical_data() RETURNS trigger AS $$
                BEGIN
                  PERFORM pg_notify('new_historical_data', NEW.id::text);
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """);
            stmt.executeUpdate("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_trigger WHERE tgname = 'app_historical_data_notify'
                    ) THEN
                        EXECUTE 'CREATE TRIGGER app_historical_data_notify AFTER INSERT OR UPDATE ON app_historical_data FOR EACH ROW EXECUTE FUNCTION notify_new_historical_data();';
                    END IF;
                END$$;
                """);
            LOGGER.info("Checked/created PostgreSQL trigger for NOTIFY on app_historical_data.");
            return true;
        } catch (Exception e) {
            LOGGER.error("Error creating trigger/function - live feed notifications will NOT work " +
                    "until this is fixed (check the DB role has CREATE privilege): {}", e.getMessage());
            return false;
        }
    }

}
