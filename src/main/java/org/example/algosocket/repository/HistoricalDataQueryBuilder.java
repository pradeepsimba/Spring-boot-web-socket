package org.example.algosocket.repository;

import org.example.algosocket.model.FilterCriteria;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pure SQL/parameter building for historical-data queries, kept free of any JDBC dependency
 * so the query-construction logic can be unit tested without a database.
 */
public final class HistoricalDataQueryBuilder {

    public static final String BASE_SELECT =
            "SELECT h.*, i.quote, i.ltp, i.snap FROM app_historical_data h LEFT JOIN app_info i ON h.info_id = i.id";

    /** Bounds any single filter list so a client can't force construction of a huge OR/IN SQL clause. */
    public static final int MAX_FILTER_LIST_SIZE = 200;

    /**
     * Hard cap applied to every non-filterObjects query, regardless of which criteria were given -
     * without this, a client-supplied wide fromTime/toTime range (or no criteria at all) could pull
     * the entire table into heap in one response.
     */
    public static final int MAX_RESULTS = 5000;

    public static final class FilterTooLargeException extends IllegalArgumentException {
        public FilterTooLargeException(String message) {
            super(message);
        }
    }

    public static final class NoFilterCriteriaException extends IllegalArgumentException {
        public NoFilterCriteriaException(String message) {
            super(message);
        }
    }

    public record SqlQuery(String sql, Object[] params) {
    }

    private HistoricalDataQueryBuilder() {
    }

    public static SqlQuery buildFind(FilterCriteria criteria) {
        validateSize(criteria.getFilterObjects());
        validateSize(criteria.getStockNames());
        validateSize(criteria.getStockSymbols());
        validateSize(criteria.getIntervals());

        if (hasFilterObjects(criteria)) {
            return buildFindByFilterObjects(criteria.getFilterObjects());
        }

        if (criteria.getFromTime() == null && criteria.getToTime() == null
                && isEmpty(criteria.getStockNames()) && isEmpty(criteria.getStockSymbols())
                && isEmpty(criteria.getIntervals())) {
            throw new NoFilterCriteriaException(
                    "At least one of filterObjects, fromTime/toTime, stockNames, stockSymbols, or intervals is required");
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
        sql.append(" ORDER BY h.start_time DESC LIMIT ").append(MAX_RESULTS);

        return new SqlQuery(sql.toString(), params.toArray());
    }

    /**
     * Returns the single most recent row per (stockname, stock_symbol, interval) among the given
     * filters, for bootstrapping a live-feed session with its current candle before live updates arrive.
     */
    public static SqlQuery buildFindLatestPerFilter(List<FilterCriteria.FilterObject> filterObjects) {
        validateSize(filterObjects);
        StringBuilder sql = new StringBuilder(
                "SELECT DISTINCT ON (h.stockname, h.stock_symbol, h.interval) h.*, i.quote, i.ltp, i.snap " +
                "FROM app_historical_data h LEFT JOIN app_info i ON h.info_id = i.id WHERE ");
        List<Object> params = new ArrayList<>();
        appendFilterObjectDisjunction(sql, params, filterObjects);
        sql.append(" ORDER BY h.stockname, h.stock_symbol, h.interval, h.start_time DESC");
        return new SqlQuery(sql.toString(), params.toArray());
    }

    private static SqlQuery buildFindByFilterObjects(List<FilterCriteria.FilterObject> filterObjects) {
        StringBuilder sql = new StringBuilder(BASE_SELECT).append(" WHERE ");
        List<Object> params = new ArrayList<>();
        appendFilterObjectDisjunction(sql, params, filterObjects);
        // No time bound here either - a bare (stockname, symbol, interval) match against a
        // fine-grained interval could otherwise return years of history in one response.
        sql.append(" ORDER BY h.start_time DESC LIMIT ").append(MAX_RESULTS);
        return new SqlQuery(sql.toString(), params.toArray());
    }

    private static void appendFilterObjectDisjunction(StringBuilder sql, List<Object> params,
                                                       List<FilterCriteria.FilterObject> filterObjects) {
        for (int i = 0; i < filterObjects.size(); i++) {
            if (i > 0) sql.append(" OR ");
            sql.append("(h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)");
            FilterCriteria.FilterObject fo = filterObjects.get(i);
            params.add(fo.getStockname());
            params.add(fo.getStockSymbol());
            params.add(fo.getInterval());
        }
    }

    private static void appendInClause(StringBuilder sql, List<Object> params, String column, List<String> values) {
        if (values == null || values.isEmpty()) return;
        String placeholders = String.join(",", Collections.nCopies(values.size(), "?"));
        sql.append(" AND ").append(column).append(" IN (").append(placeholders).append(")");
        params.addAll(values);
    }

    private static boolean hasFilterObjects(FilterCriteria criteria) {
        return criteria.getFilterObjects() != null && !criteria.getFilterObjects().isEmpty();
    }

    private static boolean isEmpty(List<?> values) {
        return values == null || values.isEmpty();
    }

    private static void validateSize(List<?> values) {
        if (values != null && values.size() > MAX_FILTER_LIST_SIZE) {
            throw new FilterTooLargeException("Filter list exceeds maximum of " + MAX_FILTER_LIST_SIZE + " entries");
        }
    }
}
