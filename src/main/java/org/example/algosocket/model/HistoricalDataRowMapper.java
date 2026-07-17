package org.example.algosocket.model;

import java.sql.ResultSet;
import java.sql.SQLException;

/** Shared row mapping for the `app_historical_data JOIN app_info` result shape used by both the repository and the live-notification listener. */
public final class HistoricalDataRowMapper {

    private HistoricalDataRowMapper() {
    }

    public static HistoricalData mapRow(ResultSet rs) throws SQLException {
        HistoricalData data = new HistoricalData(
                rs.getString("stockname"),
                rs.getString("stock_symbol"),
                rs.getString("interval"),
                toLocalDateTime(rs.getTimestamp("start_time")),
                rs.getObject("open", Double.class),
                rs.getObject("high", Double.class),
                rs.getObject("low", Double.class),
                rs.getObject("close", Double.class),
                rs.getObject("volume", Long.class),
                toLocalDateTime(rs.getTimestamp("updated_at")),
                rs.getString("quote"),
                rs.getString("ltp"),
                rs.getString("snap")
        );
        data.setId(rs.getObject("id", Long.class));
        return data;
    }

    private static java.time.LocalDateTime toLocalDateTime(java.sql.Timestamp ts) {
        return ts != null ? ts.toLocalDateTime() : null;
    }
}
