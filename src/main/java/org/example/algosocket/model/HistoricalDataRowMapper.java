package org.example.algosocket.model;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Shared row mapping for app_historical_data, used by both the repository and the live-notification
 * listener. Works against either query shape HistoricalDataQueryBuilder produces: rows either carry
 * real quote/ltp/snap values from a LEFT JOIN app_info (live/bootstrap paths), or NULL literals in
 * their place (general historical queries, where app_info's current-only values don't apply) -
 * ResultSet.getString reads by column label either way, so no special-casing is needed here.
 */
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
