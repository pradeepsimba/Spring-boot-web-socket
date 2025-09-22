package org.example.algosocket.service;

import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

@Service
public class PostgresNotificationListener implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(PostgresNotificationListener.class.getName());

    @Autowired
    private HistoricalDataWebSocketHandler webSocketHandler;

    private Thread listenerThread;
    private volatile boolean running = true;

    @PostConstruct
    public void start() {
        listenerThread = new Thread(this, "PG-Notification-Listener");
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
        try {
            String dbUrl = System.getProperty("spring.datasource.url");
            String dbUser = System.getProperty("spring.datasource.username");
            String dbPass = System.getProperty("spring.datasource.password");
            if (dbUrl == null || dbUser == null || dbPass == null) {
                dbUrl = System.getenv("SPRING_DATASOURCE_URL");
                dbUser = System.getenv("SPRING_DATASOURCE_USERNAME");
                dbPass = System.getenv("SPRING_DATASOURCE_PASSWORD");
            }
            if (dbUrl == null || dbUser == null || dbPass == null) {
                dbUrl = "jdbc:postgresql://35.244.28.3:5432/algo?sessionTimezone=Asia/Kolkata";
                dbUser = "postgres";
                dbPass = "ab45cd12";
            }
            Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            Statement stmt = conn.createStatement();

            // Create trigger function and trigger only for app_historical_data
            try {
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
                LOGGER.info("Checked/created PostgreSQL trigger and function for NOTIFY on app_historical_data only.");
            } catch (Exception e) {
                LOGGER.warning("Error creating trigger/function: " + e.getMessage());
            }

            PGConnection pgConn = conn.unwrap(PGConnection.class);
            stmt.execute("LISTEN new_historical_data");
            LOGGER.info("Started listening for PostgreSQL notifications on 'new_historical_data'.");
            while (running) {
                org.postgresql.PGNotification[] notifications = pgConn.getNotifications();
                if (notifications != null) {
                    for (org.postgresql.PGNotification notification : notifications) {
                        String payload = notification.getParameter();
                        LOGGER.info("Received NOTIFY: " + payload);
                        try {
                            ResultSet rs = stmt.executeQuery(
                                "SELECT h.*, i.quote, i.ltp, i.snap " +
                                "FROM app_historical_data h " +
                                "LEFT JOIN app_info i ON h.info_id = i.id " +
                                "WHERE h.id = " + payload
                            );
                            if (rs.next()) {
                                HistoricalData data = new HistoricalData(
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
                                webSocketHandler.broadcastRealTimeData(data);
                            }
                        } catch (Exception e) {
                            LOGGER.warning("Error fetching/broadcasting new data: " + e.getMessage());
                        }
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            LOGGER.severe("PostgresNotificationListener error: " + e.getMessage());
        }
    }
}