package org.example.algosocket.service;

import org.example.algosocket.model.HistoricalDataRowMapper;
import org.example.algosocket.repository.HistoricalDataQueryBuilder;
import org.example.algosocket.websocket.HistoricalDataWebSocketHandler;
import org.example.algosocket.model.HistoricalData;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class PostgresNotificationListener implements Runnable, ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresNotificationListener.class);

    private static final String FETCH_BY_IDS_SQL_PREFIX =
            HistoricalDataQueryBuilder.BASE_SELECT + " WHERE h.id IN (";

    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final long NOTIFICATION_POLL_TIMEOUT_MS = 1_000;

    private final HistoricalDataWebSocketHandler webSocketHandler;
    // The live-feed-dedicated pool (see LiveFeedDataSourceConfig) - NOT the main spring.datasource
    // pool JdbcTemplate uses. Supplies both this listener's one permanent LISTEN connection and
    // every parallel fetch worker's own borrowed connection (see fetchChunk).
    private final DataSource dataSource;

    // Same bound as spring.jdbc.template.query-timeout, reused here because fetchAndBroadcast uses
    // a raw JDBC PreparedStatement rather than the auto-configured JdbcTemplate that property binds
    // to - without setting it explicitly on this statement too, a stuck fetch (lock contention, a
    // long-running competing write) has no timeout at all and stalls this single-threaded listen
    // loop indefinitely, silently starving every notification behind it.
    @Value("${spring.jdbc.template.query-timeout:10}")
    private int queryTimeoutSeconds;

    // How many chunks of one NOTIFY batch may be fetched from Postgres concurrently. The DB round
    // trip used to be this pipeline's serial bottleneck: one chunk's query had to fully return
    // before the next chunk's query could even start, even though the chunks are independent reads
    // with no ordering requirement between each other (ordering only matters once results are
    // broadcast - see fetchAndBroadcast's sort). Must stay below the live-feed pool size, which
    // reserves one slot for this listener's own permanent LISTEN connection.
    @Value("${livefeed.fetch-parallelism:4}")
    private int fetchParallelism;

    private ExecutorService fetchExecutor;

    @PostConstruct
    public void initFetchExecutor() {
        fetchExecutor = Executors.newFixedThreadPool(Math.max(1, fetchParallelism));
    }

    private Thread listenerThread;
    private volatile boolean running = true;
    private volatile boolean connected = false;
    private volatile boolean triggerConfirmed = false;
    // Once the trigger/function have been confirmed present on ANY connection, skip re-running the
    // DDL on every subsequent reconnect - a transient network blip used to re-execute
    // CREATE OR REPLACE FUNCTION (invalidating dependent plan caches) and the trigger-existence
    // check/create on every single reconnect, not just at startup.
    private volatile boolean triggerEverConfirmed = false;

    public PostgresNotificationListener(HistoricalDataWebSocketHandler webSocketHandler,
                                         @Qualifier("liveFeedDataSource") DataSource dataSource) {
        this.webSocketHandler = webSocketHandler;
        this.dataSource = dataSource;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        start();
    }

    public void start() {
        // ContextRefreshedEvent is documented to fire once per context refresh - normally exactly
        // once - but guard against a second listener thread ever running concurrently (some
        // Actuator/DevTools restart paths, multi-context setups) rather than assume it. Two live
        // listeners would double-broadcast every tick and double the trigger-creation race below.
        if (listenerThread != null && listenerThread.isAlive()) {
            LOGGER.warn("start() called while a listener thread is already running - ignoring.");
            return;
        }
        listenerThread = new Thread(this, "PG-Notification-Listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (listenerThread != null) {
            listenerThread.interrupt();
            // Without joining, @PreDestroy could return - and the Spring context (and its
            // DataSource) start tearing down - while this thread is still mid-listenOnce(), racing
            // a connection close against the DataSource shutting down under it. Bounded so a thread
            // stuck in a slow query doesn't hang application shutdown indefinitely.
            try {
                listenerThread.join(2_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (fetchExecutor != null) {
            fetchExecutor.shutdown();
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

    // Reset by listenOnce() the moment a connection is successfully established, so backoff only
    // grows across consecutive failures and starts fresh after any healthy connection. (Resetting
    // it after listenOnce() returns would be dead code: the inner loop only exits on shutdown.)
    private volatile long backoffMs = INITIAL_BACKOFF_MS;

    @Override
    public void run() {
        while (running) {
            try {
                listenOnce();
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
            triggerConfirmed = triggerEverConfirmed || createTriggerIfNeeded(conn);
            if (triggerConfirmed) {
                triggerEverConfirmed = true;
            } else {
                // LISTEN would "succeed" but no NOTIFY could ever fire, leaving a silently-dead
                // feed for the life of this connection. Throw instead so run() retries with
                // backoff - a transient permission/lock failure then self-heals on a later attempt.
                throw new IllegalStateException(
                        "NOTIFY trigger/function could not be created; retrying via reconnect loop");
            }

            PGConnection pgConn = conn.unwrap(PGConnection.class);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN new_historical_data");
            }
            connected = true;
            backoffMs = INITIAL_BACKOFF_MS; // healthy connection established - reset backoff
            LOGGER.info("Started listening for PostgreSQL notifications on 'new_historical_data'.");

            while (running) {
                PGNotification[] notifications = pgConn.getNotifications((int) NOTIFICATION_POLL_TIMEOUT_MS);
                if (notifications == null || notifications.length == 0) continue;

                // LinkedHashSet, not a List: the NOTIFY trigger fires on every INSERT OR UPDATE, and
                // a single in-progress candle gets updated repeatedly as ticks arrive within its
                // interval window - same row id, many notifications, all within the same 1s poll.
                // A re-SELECT always returns that row's CURRENT state regardless of which duplicate
                // triggered it, so processing the same id more than once here is pure waste - not
                // just inefficient but the actual cause of unbounded lag growth under real trading
                // volume: with hundreds of actively-ticking (stock, interval) combos each firing
                // multiple redundant NOTIFYs per second, the un-deduplicated id list can run into
                // the thousands per poll, and this loop must fully drain the current poll's ids
                // before it can call getNotifications() again - so a growing backlog compounds
                // instead of draining, and every extra second of lag is extra staleness the
                // eventual broadcast delivers to clients (or, if a client already disconnected by
                // then, just work for nobody).
                Set<Long> ids = new LinkedHashSet<>(notifications.length);
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

                fetchAndBroadcast(new ArrayList<>(ids));
            }
        }
    }

    /** Above this, a batch's fetch-and-broadcast lag is logged at WARN instead of DEBUG. */
    private static final long LAG_WARN_THRESHOLD_MS = 5_000;

    // Caps the IN-clause size of any single fetch query. Unlike client-supplied filters (bounded
    // via MAX_FILTER_LIST_SIZE/MAX_FILTER_OBJECTS), the number of ids pulled out of one
    // getNotifications() poll has no cap of its own - a burst (a bulk backfill, a reconnect-
    // triggered replay hitting the trigger, a busy trading session) could hand this an arbitrarily
    // large list, building one very large single query. Chunking bounds worst-case query size
    // regardless of burst size, at the cost of a few more round trips for a big burst.
    private static final int MAX_FETCH_BATCH_SIZE = 500;

    private void fetchAndBroadcast(List<Long> ids) {
        List<List<Long>> chunks = new ArrayList<>();
        for (int start = 0; start < ids.size(); start += MAX_FETCH_BATCH_SIZE) {
            chunks.add(ids.subList(start, Math.min(start + MAX_FETCH_BATCH_SIZE, ids.size())));
        }

        // Fetch every chunk concurrently (each on its own borrowed connection - see fetchChunk).
        // The DB round trip used to be this pipeline's serial bottleneck: one chunk's query had to
        // fully return before the next chunk's query could even start, even though the chunks are
        // independent reads with no ordering requirement between each other.
        List<CompletableFuture<List<HistoricalData>>> futures = new ArrayList<>(chunks.size());
        for (List<Long> chunk : chunks) {
            futures.add(CompletableFuture.supplyAsync(() -> fetchChunk(chunk), fetchExecutor));
        }

        List<HistoricalData> rows = new ArrayList<>(ids.size());
        for (CompletableFuture<List<HistoricalData>> future : futures) {
            try {
                rows.addAll(future.join());
            } catch (Exception e) {
                LOGGER.warn("Error fetching a chunk of live data: {}", e.getMessage());
            }
        }

        // Fetching out of order is fine; broadcasting out of order is not. Two rows sharing the
        // same (stockname, stock_symbol, interval) - e.g. an old candle closing and a new one
        // opening at the same interval boundary, which happens for every actively-ticking combo at
        // every minute mark - must always reach a subscribed client in the order they actually
        // happened. Sorting by id (insertion order) before broadcasting guarantees that regardless
        // of which chunk's fetch happened to finish first.
        rows.sort(Comparator.comparingLong(HistoricalData::getId));

        long maxLagMs = 0;
        for (HistoricalData data : rows) {
            // updated_at is stamped by the writer at insert/update time, so now - updated_at
            // approximates end-to-end lag (write -> NOTIFY -> this fetch) - the metric that matters
            // for "is the live feed keeping up", not just how long this one query took.
            if (data.getUpdatedAt() != null) {
                long lagMs = Duration.between(data.getUpdatedAt(), LocalDateTime.now()).toMillis();
                if (lagMs > maxLagMs) maxLagMs = lagMs;
            }
            webSocketHandler.broadcastRealTimeData(data);
        }

        if (maxLagMs > LAG_WARN_THRESHOLD_MS) {
            LOGGER.warn("Notification-to-broadcast lag reached {} ms for a batch of {} row(s) - " +
                    "the live feed may be falling behind.", maxLagMs, ids.size());
        } else {
            LOGGER.debug("Notification-to-broadcast lag: {} ms for a batch of {} row(s).", maxLagMs, ids.size());
        }
    }

    /**
     * Fetches one chunk on its own connection, borrowed from the live-feed pool - deliberately NOT
     * this listener's dedicated LISTEN connection, which must stay free to keep polling
     * notifications, and which a single JDBC Connection couldn't safely serve to concurrently
     * running fetch workers anyway.
     */
    private List<HistoricalData> fetchChunk(List<Long> ids) {
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = FETCH_BY_IDS_SQL_PREFIX + placeholders + ")";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            for (int i = 0; i < ids.size(); i++) {
                ps.setLong(i + 1, ids.get(i));
            }
            List<HistoricalData> result = new ArrayList<>(ids.size());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(HistoricalDataRowMapper.mapRow(rs));
                }
            }
            return result;
        } catch (Exception e) {
            LOGGER.warn("Error fetching live data for ids {}: {}", ids, e.getMessage());
            return List.of();
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
                        BEGIN
                            EXECUTE 'CREATE TRIGGER app_historical_data_notify AFTER INSERT OR UPDATE ON app_historical_data FOR EACH ROW EXECUTE FUNCTION notify_new_historical_data();';
                        EXCEPTION WHEN duplicate_object THEN
                            -- Another instance's connection won the race and created it between
                            -- our existence check above and this CREATE (e.g. two pods starting up
                            -- at once, or even this same app racing a fast reconnect) - the trigger
                            -- exists either way, so treat this exactly like the IF branch not
                            -- firing at all instead of surfacing it as a failure.
                            NULL;
                        END;
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
