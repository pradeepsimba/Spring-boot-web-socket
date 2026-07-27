package org.example.algosocket.repository;

import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.model.HistoricalDataRowMapper;
import org.example.algosocket.repository.HistoricalDataQueryBuilder.SqlQuery;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class HistoricalDataRepository {

    private final JdbcTemplate jdbcTemplate;
    // Dedicated live-feed pool (see LiveFeedDataSourceConfig) - findLatestPerFilter is the
    // WebSocket bootstrap-snapshot query, fired in bursts correlated with WS connect/reconnect
    // storms, not a one-shot REST call like find() below. Isolating it here means a reconnect
    // storm can no longer compete with unrelated REST traffic for the same small default pool.
    private final JdbcTemplate liveFeedJdbcTemplate;

    // Both explicitly @Qualifier'd even though the first would likely also resolve correctly via
    // Spring's parameter-name-matching fallback (the Spring Boot Gradle plugin enables -parameters
    // by default) - with two JdbcTemplate beans now in the context, relying on that implicit
    // compiler-flag-dependent fallback for the default bean isn't worth the doubt when an explicit
    // qualifier costs nothing and removes it entirely.
    public HistoricalDataRepository(@Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate,
                                     @Qualifier("liveFeedJdbcTemplate") JdbcTemplate liveFeedJdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.liveFeedJdbcTemplate = liveFeedJdbcTemplate;
    }

    public List<HistoricalData> find(FilterCriteria criteria) {
        SqlQuery query = HistoricalDataQueryBuilder.buildFind(criteria);
        return jdbcTemplate.query(query.sql(), (rs, rowNum) -> HistoricalDataRowMapper.mapRow(rs), query.params());
    }

    public List<HistoricalData> findLatestPerFilter(List<FilterCriteria.FilterObject> filterObjects) {
        SqlQuery query = HistoricalDataQueryBuilder.buildFindLatestPerFilter(filterObjects);
        return liveFeedJdbcTemplate.query(query.sql(), (rs, rowNum) -> HistoricalDataRowMapper.mapRow(rs), query.params());
    }
}
