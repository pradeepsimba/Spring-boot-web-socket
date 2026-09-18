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
    // getNotifications(timeout) is a blocking socket read, not a periodic poll - it returns the
    // instant a NOTIFY arrives, so this value does NOT add latency to real notification delivery
    // during actual trading activity. It only bounds how long the loop can sit idle (affecting how
    // soon `running`/the keepalive check gets re-evaluated) during a genuinely quiet gap with zero
    // notifications. Lowered from 1000ms as a zero-downside safety margin, not because it was
    // actually delaying live ticks.
    private static final long NOTIFICATION_POLL_TIMEOUT_MS = 200;

    // How often, while otherwise idle inside the LISTEN poll loop, this connection is actively
    // probed for liveness. getNotifications()/LISTEN rely entirely on an I/O exception to detect a
    // dead connection - but a connection silently dropped by a NAT/firewall/load-balancer idle
    // timeout (no RST sent) produces no I/O exception at all: getNotifications() just keeps
    // returning null/empty forever, indistinguishable from a healthy-but-quiet feed. Running a
    // trivial round trip on this schedule forces that failure to surface promptly instead of never.
    private static final long KEEPALIVE_INTERVAL_MS = 30_000;

    // Hard cap on the number of rows a single post-reconnect catch-up query may return. Postgres
    // NOTIFY is fire-and-forget and not queued for a disconnected/not-yet-listening client, so any
    // insert/update that happened during a dropped connection would otherwise be silently and
    // permanently lost - this bounds that one recovery query the same way MAX_FETCH_BATCH_SIZE
    // bounds a normal notification batch, so an unusually long outage can't build one huge query.
    private static final int CATCHUP_MAX_ROWS = 5_000;

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
            // Without awaiting, this method (and therefore @PreDestroy) could return while a
            // fetchChunk task is still mid-executeQuery() on a connection borrowed from
            // liveFeedDataSource - Spring's destroy-order guarantee covers bean destroy() calls,
            // not background threads a bean spawned, so the live-feed pool could be closed by the
            // container right out from under that still-running query. Bounded so a stuck fetch
            // can't hang shutdown indefinitely.
            try {
                if (!fetchExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
                    fetchExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                fetchExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
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

    // Marks how far this listener has caught up: the latest updated_at it has either (a) actually
    // processed via a normal NOTIFY-driven fetch, or (b) established as a safe starting point at the
    // moment a LISTEN connection first came up (see listenOnce). Only ever read/written from this
    // listener's own single thread (run()/listenOnce()/fetchAndBroadcast() all execute serially on
    // it - fetchChunk's futures are joined before fetchAndBroadcast returns), so no synchronization
    // is needed beyond volatile visibility for isConnected()-style external reads. Null only before
    // this listener's very first successful connection, when there is nothing yet to catch up from.
    private volatile LocalDateTime lastProcessedMarker;

    @Override
    public void run() {
        while (running) {
            try {
                listenOnce();
            } catch (Exception e) {
                LOGGER.warn("PostgresNotificationListener connection lost: {}", e.getMessage(), e);
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

            // Catch up on anything that changed while this listener was disconnected (network blip,
            // DB restart, dropped connection) - NOTIFY gives no queue/replay for a listener that
            // wasn't connected/listening at the time, so without this, a row inserted/updated during
            // that gap would never be broadcast at all, with no signal anything was missed. Skipped
            // only on this listener's very first-ever connection, when there is no prior marker and
            // therefore nothing yet to have missed.
            LocalDateTime catchUpSince = lastProcessedMarker;
            LocalDateTime listenEstablishedAt = LocalDateTime.now();
            if (catchUpSince != null) {
                runCatchUp(catchUpSince);
            }
            lastProcessedMarker = listenEstablishedAt;

            long lastKeepAliveAtMs = System.currentTimeMillis();
            while (running) {
                PGNotification[] notifications = pgConn.getNotifications((int) NOTIFICATION_POLL_TIMEOUT_MS);

                long nowMs = System.currentTimeMillis();
                if (nowMs - lastKeepAliveAtMs >= KEEPALIVE_INTERVAL_MS) {
                    // Forces a real round trip on this connection so a silent drop (no RST, e.g. a
                    // NAT/firewall/LB idle timeout) is detected here instead of leaving the listener
                    // believing it's still connected indefinitely with a dead feed - getNotifications()
                    // alone would keep returning null/empty forever in that scenario. isValid() issues
                    // exactly the trivial no-op round trip this is meant to be.
                    if (!conn.isValid(queryTimeoutSeconds)) {
                        throw new java.sql.SQLException(
                                "Live-feed LISTEN connection failed keepalive check (isValid returned false)");
                    }
                    lastKeepAliveAtMs = nowMs;
                }

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
                LOGGER.warn("Error fetching a chunk of live data: {}", e.getMessage(), e);
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
        LocalDateTime maxUpdatedAt = null;
        for (HistoricalData data : rows) {
            // updated_at is stamped by the writer at insert/update time, so now - updated_at
            // approximates end-to-end lag (write -> NOTIFY -> this fetch) - the metric that matters
            // for "is the live feed keeping up", not just how long this one query took.
            if (data.getUpdatedAt() != null) {
                long lagMs = Duration.between(data.getUpdatedAt(), LocalDateTime.now()).toMillis();
                if (lagMs > maxLagMs) maxLagMs = lagMs;
                if (maxUpdatedAt == null || data.getUpdatedAt().isAfter(maxUpdatedAt)) {
                    maxUpdatedAt = data.getUpdatedAt();
                }
            }
            webSocketHandler.broadcastRealTimeData(data);
        }

        // Advance the catch-up marker as normal notifications are processed (not just at connection
        // start) so that if THIS connection later drops, the next reconnect's catch-up query only
        // has to cover the true gap since the last row actually seen, rather than redoing this
        // entire (possibly hours-long) connected session's worth of already-broadcast rows.
        if (maxUpdatedAt != null && (lastProcessedMarker == null || maxUpdatedAt.isAfter(lastProcessedMarker))) {
            lastProcessedMarker = maxUpdatedAt;
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
            consecutiveFetchFailures.set(0);
            return result;
        } catch (Exception e) {
            // This borrows from the SAME liveFeedDataSource pool the LISTEN connection uses but is
            // otherwise invisible to isConnected() - a pool exhausted/stuck here would silently
            // drop every NOTIFY batch it touches while the health check kept reporting UP (the
            // LISTEN connection is a different pool slot with different failure characteristics).
            // Tracked so LiveFeedHealthIndicator can surface this failure mode too.
            consecutiveFetchFailures.incrementAndGet();
            LOGGER.warn("Error fetching live data for ids {}: {}", ids, e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * After a (re)connect, runs one bounded query for rows whose updated_at is after {@code since}
     * and feeds the resulting ids through the same {@link #fetchAndBroadcast} path used for normal
     * notifications - see the KEEPALIVE_INTERVAL_MS/CATCHUP_MAX_ROWS javadoc above and the call site
     * in listenOnce for why this exists. Runs on a connection borrowed from the live-feed pool
     * (mirrors fetchChunk), never the dedicated LISTEN connection, which must stay free to keep
     * polling notifications.
     */
    private void runCatchUp(LocalDateTime since) {
        String sql = "SELECT h.id FROM app_historical_data h WHERE h.updated_at > ? "
                + "ORDER BY h.updated_at ASC LIMIT " + CATCHUP_MAX_ROWS;
        List<Long> ids = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setObject(1, since);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong("id"));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Catch-up query for rows changed since {} failed - any updates missed while " +
                    "disconnected will NOT be recovered for this reconnect: {}", since, e.getMessage(), e);
            return;
        }

        if (ids.isEmpty()) {
            LOGGER.info("Catch-up after reconnect: no rows changed since {}.", since);
            return;
        }

        if (ids.size() >= CATCHUP_MAX_ROWS) {
            LOGGER.warn("Catch-up after reconnect hit the {}-row cap for changes since {} - some missed " +
                    "updates older than the {} most recent may not have been recovered.",
                    CATCHUP_MAX_ROWS, since, CATCHUP_MAX_ROWS);
        } else {
            LOGGER.info("Catch-up after reconnect: recovering {} row(s) changed since {}.", ids.size(), since);
        }
        fetchAndBroadcast(ids);
    }

    /** Above this many consecutive fetchChunk failures, the fetch path is reported unhealthy. */
    private static final int FETCH_FAILURE_HEALTH_THRESHOLD = 3;
    private final java.util.concurrent.atomic.AtomicInteger consecutiveFetchFailures = new java.util.concurrent.atomic.AtomicInteger();

    /** @return true unless the fetch path (separate from the LISTEN connection) has been failing repeatedly. */
    public boolean isFetchHealthy() {
        return consecutiveFetchFailures.get() < FETCH_FAILURE_HEALTH_THRESHOLD;
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
                    "until this is fixed (check the DB role has CREATE privilege): {}", e.getMessage(), e);
            return false;
        }
    }

}
