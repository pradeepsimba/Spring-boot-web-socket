package org.example.algosocket.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * A small connection pool dedicated to the live feed (PostgresNotificationListener's permanent
 * LISTEN connection, plus its parallel fetch workers) - kept separate from the main
 * spring.datasource pool (JdbcTemplate: one-shot historical queries, live-feed bootstrap
 * snapshots). Without this split, a burst of client connects/reconnects competing for the shared
 * pool could starve the live feed of a connection, and vice versa. Not marked @Primary, so
 * Spring Boot's autoconfigured DataSource/JdbcTemplate (from spring.datasource.*) is untouched.
 */
@Configuration
public class LiveFeedDataSourceConfig {

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
