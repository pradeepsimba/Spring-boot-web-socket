package org.example.algosocket.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * A small connection pool dedicated to the live feed - PostgresNotificationListener's permanent
 * LISTEN connection and its parallel fetch workers, AND the WebSocket bootstrap-snapshot query
 * (HistoricalDataRepository.findLatestPerFilter, via liveFeedJdbcTemplate below). Kept separate
 * from the main spring.datasource pool (which now only serves one-shot REST historical queries).
 * Without this split, a burst of client connects/reconnects - each firing one bootstrap-snapshot
 * query, up to ~125 concurrently at the current 300-subscription-per-connection cap - would
 * compete with unrelated REST traffic for the same small pool, and vice versa.
 *
 * IMPORTANT: this class must ALSO define the main `dataSource`/`jdbcTemplate` beans itself, marked
 * @Primary, rather than leaving them to Spring Boot's autoconfiguration. Boot's
 * DataSourceAutoConfiguration/JdbcTemplateAutoConfiguration gate their own beans with
 * @ConditionalOnMissingBean(DataSource.class)/@ConditionalOnMissingBean(JdbcOperations.class) -
 * that match is by TYPE, not bean name. The instant liveFeedDataSource/liveFeedJdbcTemplate exist
 * anywhere in the context (under any name), those conditions see a DataSource/JdbcOperations bean
 * already present and skip creating the autoconfigured default entirely - there is then NO bean
 * to satisfy HistoricalDataRepository's `@Qualifier("jdbcTemplate")` constructor parameter, and
 * the whole application context fails to start. (Confirmed by booting the context with a real,
 * non-mocked HistoricalDataRepository - the existing smoke test @MockBeans it away, which is why
 * this went unnoticed.) Hand-rolling both beans here, exactly mirroring what
 * spring.datasource.url/username/password/hikari.* would have auto-bound, and marking the main
 * one @Primary, restores the same "one unqualified default + one named extra" shape the rest of
 * the codebase (HistoricalDataRepository's two constructor params) assumes.
 *
 * Bootstrap-snapshot queries were previously left on the default/main JdbcTemplate under the
 * reasoning that they're "one-shot" like a REST call - but they actually arrive in bursts
 * correlated with WS reconnect storms (network blips, LB restarts), which is exactly the
 * live-feed contention pattern this dedicated pool exists to isolate, not the main pool's REST
 * traffic pattern.
 */
@Configuration
public class LiveFeedDataSourceConfig {

    @Primary
    @Bean
    public DataSource dataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${spring.datasource.hikari.maximum-pool-size:15}") int maxPoolSize,
            @Value("${spring.datasource.hikari.minimum-idle:2}") int minIdle,
            @Value("${spring.datasource.hikari.connection-timeout:30000}") long connectionTimeoutMs) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setMinimumIdle(minIdle);
        ds.setConnectionTimeout(connectionTimeoutMs);
        ds.setPoolName("main-pool");
        return ds;
    }

    @Primary
    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public JdbcTemplate liveFeedJdbcTemplate(DataSource liveFeedDataSource) {
        return new JdbcTemplate(liveFeedDataSource);
    }

    @Bean
    public DataSource liveFeedDataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${livefeed.datasource.hikari.maximum-pool-size:8}") int maxPoolSize,
            @Value("${livefeed.datasource.hikari.minimum-idle:2}") int minIdle,
            @Value("${spring.datasource.hikari.connection-timeout:30000}") long connectionTimeoutMs) {
        // Deliberately the no-arg constructor + setters, NOT `new HikariDataSource(HikariConfig)`:
        // the config-argument constructor eagerly initializes the pool (opens a real connection)
        // right here in this @Bean method, which would make application context startup itself
        // fail whenever the DB is briefly unreachable - exactly the failure mode Spring Boot's own
        // autoconfigured DataSource avoids by using this same no-arg-then-setters pattern
        // internally (pool creation, and any real connection attempt, is deferred to first use).
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setMinimumIdle(minIdle);
        ds.setConnectionTimeout(connectionTimeoutMs);
        ds.setPoolName("live-feed-pool");
        return ds;
    }
}
