package org.example.algosocket.repository;

import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Repository
public class HistoricalDataRepository {

    private static final String BASE_SELECT =
            "SELECT h.*, i.quote, i.ltp, i.snap FROM app_historical_data h LEFT JOIN app_info i ON h.info_id = i.id";

    private final JdbcTemplate jdbcTemplate;

    public HistoricalDataRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public LocalDateTime findLatestUpdateTime() {
        return jdbcTemplate.queryForObject("SELECT MAX(updated_at) FROM app_historical_data", LocalDateTime.class);
    }

    public List<HistoricalData> findAll() {
        return jdbcTemplate.query(BASE_SELECT, (rs, rowNum) -> mapRow(rs));
    }

    public List<HistoricalData> find(FilterCriteria criteria) {
        if (hasFilterObjects(criteria)) {
            return findByFilterObjects(criteria.getFilterObjects());
        }

        StringBuilder sql = new StringBuilder(BASE_SELECT).append(" WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (criteria.getFromTime() != null) {
            sql.append(" AND h.start_time >= ?");
            params.add(criteria.getFromTime());
        }
        if (criteria.getToTime() != null) {
            sql.append(" AND h.start_time <= ?");
            params.add(criteria.getToTime());
        }
        appendInClause(sql, params, "h.stockname", criteria.getStockNames());
        appendInClause(sql, params, "h.stock_symbol", criteria.getStockSymbols());
        appendInClause(sql, params, "h.interval", criteria.getIntervals());

        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> mapRow(rs), params.toArray());
    }

    public List<HistoricalData> findLatest(FilterCriteria criteria, LocalDateTime since) {
        StringBuilder sql = new StringBuilder(BASE_SELECT).append(" WHERE h.updated_at > ?");
        List<Object> params = new ArrayList<>();
        params.add(since);

        if (hasFilterObjects(criteria)) {
            appendFilterObjectConditions(sql, params, criteria.getFilterObjects());
        } else {
            appendInClause(sql, params, "h.stockname", criteria.getStockNames());
        }

        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> mapRow(rs), params.toArray());
    }

    private List<HistoricalData> findByFilterObjects(List<FilterCriteria.FilterObject> filterObjects) {
        StringBuilder sql = new StringBuilder(BASE_SELECT).append(" WHERE ");
        List<Object> params = new ArrayList<>();

        for (int i = 0; i < filterObjects.size(); i++) {
            if (i > 0) sql.append(" OR ");
            sql.append("(h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)");
            FilterCriteria.FilterObject fo = filterObjects.get(i);
            params.add(fo.getStockname());
            params.add(fo.getStockSymbol());
            params.add(fo.getInterval());
        }

        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> mapRow(rs), params.toArray());
    }

    private void appendFilterObjectConditions(StringBuilder sql, List<Object> params,
                                               List<FilterCriteria.FilterObject> filterObjects) {
        sql.append(" AND (");
        for (int i = 0; i < filterObjects.size(); i++) {
            if (i > 0) sql.append(" OR ");
            sql.append("(h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)");
            FilterCriteria.FilterObject fo = filterObjects.get(i);
            params.add(fo.getStockname());
            params.add(fo.getStockSymbol());
            params.add(fo.getInterval());
        }
        sql.append(")");
    }

    private void appendInClause(StringBuilder sql, List<Object> params, String column, List<String> values) {
        if (values == null || values.isEmpty()) return;
        String placeholders = String.join(",", Collections.nCopies(values.size(), "?"));
        sql.append(" AND ").append(column).append(" IN (").append(placeholders).append(")");
        params.addAll(values);
    }

    private boolean hasFilterObjects(FilterCriteria criteria) {
        return criteria.getFilterObjects() != null && !criteria.getFilterObjects().isEmpty();
    }

    private HistoricalData mapRow(ResultSet rs) throws SQLException {
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
