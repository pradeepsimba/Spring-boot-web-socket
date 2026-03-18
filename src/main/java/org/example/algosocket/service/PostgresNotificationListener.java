package org.example.algosocket.service;

import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

@Service
public class PostgresNotificationListener implements Runnable, ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOGGER = Logger.getLogger(PostgresNotificationListener.class.getName());

    private static final String FETCH_BY_ID_SQL =
            "SELECT h.*, i.quote, i.ltp, i.snap " +
            "FROM app_historical_data h " +
            "LEFT JOIN app_info i ON h.info_id = i.id " +
            "WHERE h.id = ?";

    private final HistoricalDataWebSocketHandler webSocketHandler;
    private final DataSource dataSource;

    private Thread listenerThread;
    private volatile boolean running = true;

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

    @Override
    public void run() {
        try (Connection conn = dataSource.getConnection()) {
            createTriggerIfNeeded(conn);

            PGConnection pgConn = conn.unwrap(PGConnection.class);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN new_historical_data");
            }
            LOGGER.info("Started listening for PostgreSQL notifications on 'new_historical_data'.");

            while (running) {
                PGNotification[] notifications = pgConn.getNotifications(1000);
                if (notifications == null) continue;

                try (PreparedStatement ps = conn.prepareStatement(FETCH_BY_ID_SQL)) {
                    for (PGNotification notification : notifications) {
                        String payload = notification.getParameter();
                        LOGGER.info("Received NOTIFY: " + payload);
                        try {
                            long id = Long.parseLong(payload);
                            ps.setLong(1, id);
                            try (ResultSet rs = ps.executeQuery()) {
                                if (rs.next()) {
                                    HistoricalData data = mapRow(rs);
                                    webSocketHandler.broadcastRealTimeData(data);
                                }
                            }
                        } catch (Exception e) {
                            LOGGER.warning("Error fetching/broadcasting new data: " + e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.severe("PostgresNotificationListener error: " + e.getMessage());
        }
    }

    private void createTriggerIfNeeded(Connection conn) {
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
        } catch (Exception e) {
            LOGGER.warning("Error creating trigger/function: " + e.getMessage());
        }
    }

    private HistoricalData mapRow(ResultSet rs) throws java.sql.SQLException {
        return new HistoricalData(
                rs.getString("stockname"),
                rs.getString("stock_symbol"),
                rs.getString("interval"),
                rs.getTimestamp("start_time").toLocalDateTime(),
                rs.getDouble("open"),
                rs.getDouble("high"),
                rs.getDouble("low"),
                rs.getDouble("close"),
                rs.getLong("volume"),
                rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").toLocalDateTime() : null,
                rs.getString("quote"),
                rs.getString("ltp"),
                rs.getString("snap")
        );
    }
}
