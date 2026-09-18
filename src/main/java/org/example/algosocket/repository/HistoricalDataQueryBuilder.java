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

    /**
     * Joins app_info for its CURRENT quote/ltp/snap values - correct only when the candle row IS
     * "now": PostgresNotificationListener's live-broadcast fetch (a row that was just inserted/
     * updated) and buildFindLatestPerFilter below (explicitly "the current candle"). Reused by
     * PostgresNotificationListener.FETCH_BY_IDS_SQL_PREFIX - do not repurpose for historical
     * queries; see HISTORICAL_SELECT_RANKED for why.
     */
    public static final String BASE_SELECT =
            "SELECT h.*, i.quote, i.ltp, i.snap FROM app_historical_data h LEFT JOIN app_info i ON h.info_id = i.id";

    /**
     * For genuinely historical queries (buildFind/buildFindByFilterObjects, i.e. the one-shot
     * "historical data" query path, as opposed to the live feed). app_info holds only the LATEST
     * quote/ltp/snap for a stock - joining it here would attach TODAY's live quote to a candle row
     * from weeks ago, which is wrong, not just imprecise. Point-in-time snapshots do exist now in
     * app_info_history, but a correct join against it needs 3 correlated LATERAL subqueries per row
     * and no current caller consumes these fields on this path - so return null rather than either
     * silently-wrong or speculatively-expensive data. Revisit if a caller actually needs point-in-
     * time quote/ltp/snap attached to historical rows.
     *
     * <p>Carries a ROW_NUMBER() window column (partitioned by (stockname, stock_symbol, interval),
     * ordered by start_time DESC) so buildFind/buildFindByFilterObjects can apply MAX_RESULTS PER
     * GROUP via {@link #wrapWithPerGroupLimit} instead of once globally - see MAX_RESULTS's javadoc
     * for why a single global LIMIT on the combined result set was a bug. Not used by
     * buildFindLatestPerFilter, which already limits itself to one row per group via DISTINCT ON and
     * has no need for a window function.
     */
    private static final String HISTORICAL_SELECT_RANKED =
            "SELECT h.*, NULL::text AS quote, NULL::text AS ltp, NULL::text AS snap, " +
            "ROW_NUMBER() OVER (PARTITION BY h.stockname, h.stock_symbol, h.interval ORDER BY h.start_time DESC) AS rn "
            + "FROM app_historical_data h";

    /**
     * Bounds any single filter list so a client can't force construction of a huge OR/IN SQL
     * clause. Kept in sync with HistoricalDataWebSocketHandler.MAX_FILTER_OBJECTS: that cap gates
     * how many filters a live-feed session may subscribe to, but buildFindLatestPerFilter (the
     * live-feed bootstrap snapshot) validates against THIS constant - if it were lower than
     * MAX_FILTER_OBJECTS, a session subscribing with the maximum allowed filter count would pass
     * the WebSocket-level check and then silently fail (a caught, logged exception) to get its
     * initial snapshot, only to start receiving live ticks with no bootstrap state.
     */
    public static final int MAX_FILTER_LIST_SIZE = 300;

    /**
     * Cap on how many rows any single (stockname, stock_symbol, interval) group may contribute to
     * buildFind/buildFindByFilterObjects' result - without this, a client-supplied wide fromTime/
     * toTime range (or no criteria at all) could pull the entire table into heap in one response.
     * Applied PER GROUP via a ROW_NUMBER() OVER (PARTITION BY ...) window, not as a single global
     * LIMIT on the whole result set: a global LIMIT let one or two high-frequency stocks/intervals
     * consume the entire budget, silently returning zero rows for every other matched group with no
     * truncation indicator - partitioning guarantees each matched group gets up to this many of its
     * own most-recent rows regardless of how many other groups also matched.
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
            return buildFindByFilterObjects(criteria);
        }

        if (criteria.getFromTime() == null && criteria.getToTime() == null
                && isEmpty(criteria.getStockNames()) && isEmpty(criteria.getStockSymbols())
                && isEmpty(criteria.getIntervals())) {
            throw new NoFilterCriteriaException(
                    "At least one of filterObjects, fromTime/toTime, stockNames, stockSymbols, or intervals is required");
        }

        StringBuilder sql = new StringBuilder(HISTORICAL_SELECT_RANKED).append(" WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (criteria.getFromTime() != null) {
            sql.append(" AND h.start_time >= ?");
            params.add(criteria.getFromTime());
        }
        if (criteria.getToTime() != null) {
            sql.append(" AND h.start_time <= ?");
            params.add(criteria.getToTime());
        }
        appendInClauseCaseInsensitive(sql, params, "h.stockname", criteria.getStockNames());
        appendInClauseCaseInsensitive(sql, params, "h.stock_symbol", criteria.getStockSymbols());
        // interval stays exact/case-sensitive, matching Django's own get_historical_data (which
        // filters interval= directly, not __iexact - interval values are fixed lowercase tokens
        // like "1m"/"5m"/"1d", never user-typed with varying case).
        appendInClause(sql, params, "h.interval", criteria.getIntervals());

        return new SqlQuery(wrapWithPerGroupLimit(sql.toString()), params.toArray());
    }

    /**
     * Returns the single most recent row per (stockname, stock_symbol, interval) among the given
     * filters, for bootstrapping a live-feed session with its current candle before live updates arrive.
     */
    public static SqlQuery buildFindLatestPerFilter(List<FilterCriteria.FilterObject> filterObjects) {
        if (filterObjects == null || filterObjects.isEmpty()) {
            // Empty list would produce "... WHERE  ORDER BY ..." (invalid SQL); reject cleanly.
            throw new NoFilterCriteriaException("buildFindLatestPerFilter requires at least one filter object");
        }
        validateSize(filterObjects);
        StringBuilder sql = new StringBuilder(
                "SELECT DISTINCT ON (h.stockname, h.stock_symbol, h.interval) h.*, i.quote, i.ltp, i.snap " +
                "FROM app_historical_data h LEFT JOIN app_info i ON h.info_id = i.id WHERE ");
        List<Object> params = new ArrayList<>();
        appendFilterObjectDisjunction(sql, params, filterObjects);
        sql.append(" ORDER BY h.stockname, h.stock_symbol, h.interval, h.start_time DESC");
        return new SqlQuery(sql.toString(), params.toArray());
    }

    private static SqlQuery buildFindByFilterObjects(FilterCriteria criteria) {
        StringBuilder sql = new StringBuilder(HISTORICAL_SELECT_RANKED).append(" WHERE (");
        List<Object> params = new ArrayList<>();
        appendFilterObjectDisjunction(sql, params, criteria.getFilterObjects());
        sql.append(")");
        // Was unconditionally ignoring fromTime/toTime (and any of stockNames/stockSymbols/
        // intervals) whenever filterObjects was also present - a caller combining filterObjects
        // with a time range (e.g. "just RELIANCE 1m, but only today") got the time bound silently
        // dropped and up to MAX_RESULTS rows of that symbol's ENTIRE history back instead, with no
        // error indicating the range was ignored. Applying the same fromTime/toTime bounds buildFind
        // uses for its non-filterObjects path closes that gap; stockNames/stockSymbols/intervals are
        // deliberately still not combined with filterObjects since a filterObject already fully
        // specifies (stockname, symbol, interval) - combining them would only ever narrow to the
        // same or fewer rows, at the cost of a much more complex query.
        if (criteria.getFromTime() != null) {
            sql.append(" AND h.start_time >= ?");
            params.add(criteria.getFromTime());
        }
        if (criteria.getToTime() != null) {
            sql.append(" AND h.start_time <= ?");
            params.add(criteria.getToTime());
        }
        return new SqlQuery(wrapWithPerGroupLimit(sql.toString()), params.toArray());
    }

    /**
     * Wraps an inner query built from {@link #HISTORICAL_SELECT_RANKED} (which must already carry
     * an {@code rn} window column) so only the top MAX_RESULTS rows of EACH (stockname,
     * stock_symbol, interval) group survive, instead of one LIMIT applied to the combined result
     * set - see MAX_RESULTS's javadoc for why a single global LIMIT is the bug this fixes.
     */
    private static String wrapWithPerGroupLimit(String innerSqlWithRankColumn) {
        return "SELECT * FROM (" + innerSqlWithRankColumn + ") ranked WHERE ranked.rn <= " + MAX_RESULTS
                + " ORDER BY ranked.start_time DESC";
    }

    private static void appendFilterObjectDisjunction(StringBuilder sql, List<Object> params,
                                                       List<FilterCriteria.FilterObject> filterObjects) {
        // stockname/stock_symbol compared via UPPER() on both sides - the Django backend's
        // get_historical_data filters these two with __iexact (case-insensitive) and even has a
        // purpose-built expression index for exactly this predicate shape
        // (app_histori_upper_covering_idx, on UPPER(stockname), UPPER(stock_symbol), interval,
        // start_time - see backend_server_algo/app/models.py). Without matching that convention
        // here, a client using the same mixed-case values the Django API explicitly supports (its
        // own docstring example is "hdfc bank") gets zero results here and never receives any live
        // ticks for that filter, silently. interval stays exact - see the comment on buildFind.
        for (int i = 0; i < filterObjects.size(); i++) {
            if (i > 0) sql.append(" OR ");
            sql.append("(UPPER(h.stockname) = UPPER(?) AND UPPER(h.stock_symbol) = UPPER(?) AND h.interval = ?)");
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

    /** Same as {@link #appendInClause}, but for columns compared case-insensitively (see
     * appendFilterObjectDisjunction's comment for why stockname/stock_symbol need this). */
    private static void appendInClauseCaseInsensitive(StringBuilder sql, List<Object> params, String column, List<String> values) {
        if (values == null || values.isEmpty()) return;
        String placeholders = String.join(",", Collections.nCopies(values.size(), "UPPER(?)"));
        sql.append(" AND UPPER(").append(column).append(") IN (").append(placeholders).append(")");
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
