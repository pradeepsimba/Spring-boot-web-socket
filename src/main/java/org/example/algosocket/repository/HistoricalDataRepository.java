package org.example.algosocket.repository;

import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.model.HistoricalDataRowMapper;
import org.example.algosocket.repository.HistoricalDataQueryBuilder.SqlQuery;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class HistoricalDataRepository {

    private final JdbcTemplate jdbcTemplate;

    public HistoricalDataRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<HistoricalData> find(FilterCriteria criteria) {
        SqlQuery query = HistoricalDataQueryBuilder.buildFind(criteria);
        return jdbcTemplate.query(query.sql(), (rs, rowNum) -> HistoricalDataRowMapper.mapRow(rs), query.params());
    }

    public List<HistoricalData> findLatestPerFilter(List<FilterCriteria.FilterObject> filterObjects) {
        SqlQuery query = HistoricalDataQueryBuilder.buildFindLatestPerFilter(filterObjects);
        return jdbcTemplate.query(query.sql(), (rs, rowNum) -> HistoricalDataRowMapper.mapRow(rs), query.params());
    }
}
