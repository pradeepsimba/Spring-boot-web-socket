package org.example.algosocket.repository;

import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Repository
public class HistoricalDataRepository {

    private final JdbcTemplate jdbcTemplate;

    public HistoricalDataRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
        public LocalDateTime findLatestUpdateTime() {
            String sql = "SELECT MAX(updated_at) FROM app_historical_data";
            return jdbcTemplate.queryForObject(sql, LocalDateTime.class);
        }

    public List<HistoricalData> findAll() {
        String sql = "SELECT h.*, i.quote, i.ltp, i.snap FROM app_historical_data h LEFT JOIN app_info i ON h.info = i.id";
        return jdbcTemplate.query(sql, (rs, rowNum) -> toHistoricalData(rs));
    }

    public List<HistoricalData> find(FilterCriteria filterCriteria) {
        // If filterObjects is present, build OR conditions for exact combinations
        if (filterCriteria.getFilterObjects() != null && !filterCriteria.getFilterObjects().isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT * FROM app_historical_data WHERE ");
            List<Object> params = new ArrayList<>();
            for (int i = 0; i < filterCriteria.getFilterObjects().size(); i++) {
                if (i > 0) sql.append(" OR ");
                sql.append("(stockname = ? AND stock_symbol = ? AND interval = ?)");
                FilterCriteria.FilterObject fo = filterCriteria.getFilterObjects().get(i);
                params.add(fo.getStockname());
                params.add(fo.getStockSymbol());
                params.add(fo.getInterval());
            }
            return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> toHistoricalData(rs));
        } else {
            StringBuilder sql = new StringBuilder("SELECT * FROM app_historical_data WHERE 1=1");
            List<Object> params = new ArrayList<>();

            if (filterCriteria.getFromTime() != null) {
                sql.append(" AND start_time >= ?");
                params.add(filterCriteria.getFromTime());
            }

            if (filterCriteria.getToTime() != null) {
                sql.append(" AND start_time <= ?");
                params.add(filterCriteria.getToTime());
            }

            if (filterCriteria.getStockNames() != null && !filterCriteria.getStockNames().isEmpty()) {
                sql.append(" AND stockname IN (");
                for (int i = 0; i < filterCriteria.getStockNames().size(); i++) {
                    sql.append("?");
                    if (i < filterCriteria.getStockNames().size() - 1) {
                        sql.append(",");
                    }
                    params.add(filterCriteria.getStockNames().get(i));
                }
                sql.append(")");
            }

            if (filterCriteria.getStockSymbols() != null && !filterCriteria.getStockSymbols().isEmpty()) {
                sql.append(" AND stock_symbol IN (");
                for (int i = 0; i < filterCriteria.getStockSymbols().size(); i++) {
                    sql.append("?");
                    if (i < filterCriteria.getStockSymbols().size() - 1) {
                        sql.append(",");
                    }
                    params.add(filterCriteria.getStockSymbols().get(i));
                }
                sql.append(")");
            }

            if (filterCriteria.getIntervals() != null && !filterCriteria.getIntervals().isEmpty()) {
                sql.append(" AND interval IN (");
                for (int i = 0; i < filterCriteria.getIntervals().size(); i++) {
                    sql.append("?");
                    if (i < filterCriteria.getIntervals().size() - 1) {
                        sql.append(",");
                    }
                    params.add(filterCriteria.getIntervals().get(i));
                }
                sql.append(")");
            }

            return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> toHistoricalData(rs));
        }
    }

    public List<HistoricalData> findLatest(FilterCriteria filterCriteria, LocalDateTime since) {
        if (filterCriteria.getFilterObjects() != null && !filterCriteria.getFilterObjects().isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT * FROM app_historical_data WHERE updated_at > ?");
            List<Object> params = new ArrayList<>();
            params.add(since);
            for (int i = 0; i < filterCriteria.getFilterObjects().size(); i++) {
                sql.append(i == 0 ? " AND (" : " OR ");
                sql.append("(stockname = ? AND stock_symbol = ? AND interval = ?)");
                FilterCriteria.FilterObject fo = filterCriteria.getFilterObjects().get(i);
                params.add(fo.getStockname());
                params.add(fo.getStockSymbol());
                params.add(fo.getInterval());
            }
            sql.append(")");
            return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> toHistoricalData(rs));
        } else {
            StringBuilder sql = new StringBuilder("SELECT * FROM app_historical_data WHERE updated_at > ?");
            List<Object> params = new ArrayList<>();
            params.add(since);

            if (filterCriteria.getStockNames() != null && !filterCriteria.getStockNames().isEmpty()) {
                sql.append(" AND stockname IN (");
                for (int i = 0; i < filterCriteria.getStockNames().size(); i++) {
                    sql.append("?");
                    if (i < filterCriteria.getStockNames().size() - 1) {
                        sql.append(",");
                    }
                    params.add(filterCriteria.getStockNames().get(i));
                }
                sql.append(")");
            }

            return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> toHistoricalData(rs));
        }
    }

    private HistoricalData toHistoricalData(ResultSet rs) throws SQLException {
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