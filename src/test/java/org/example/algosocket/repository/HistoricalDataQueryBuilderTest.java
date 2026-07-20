package org.example.algosocket.repository;

import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.repository.HistoricalDataQueryBuilder.SqlQuery;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HistoricalDataQueryBuilderTest {

    @Test
    void buildFind_withNoCriteriaAtAll_throws() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> HistoricalDataQueryBuilder.buildFind(new FilterCriteria()))
                .isInstanceOf(HistoricalDataQueryBuilder.NoFilterCriteriaException.class);
    }

    @Test
    void buildFind_withTimeRangeAndNames_appendsParamsInOrderAndCapsResults() {
        FilterCriteria criteria = new FilterCriteria();
        LocalDateTime from = LocalDateTime.of(2026, 1, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2026, 1, 2, 0, 0);
        criteria.setFromTime(from);
        criteria.setToTime(to);
        criteria.setStockNames(List.of("NIFTY 50", "TCS"));

        SqlQuery query = HistoricalDataQueryBuilder.buildFind(criteria);

        assertThat(query.sql())
                .contains("AND h.start_time >= ?")
                .contains("AND h.start_time <= ?")
                .contains("AND h.stockname IN (?,?)")
                .contains("LIMIT " + HistoricalDataQueryBuilder.MAX_RESULTS)
                // Regression guard: historical rows must NOT join app_info's always-current
                // quote/ltp/snap - that would attach today's live quote to old candle rows.
                .contains("NULL::text AS quote")
                .doesNotContain("LEFT JOIN app_info");
        assertThat(query.params()).containsExactly(from, to, "NIFTY 50", "TCS");
    }

    @Test
    void buildFind_withFilterObjects_ignoresOtherCriteriaAndBuildsDisjunction() {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setStockNames(List.of("SHOULD BE IGNORED"));
        criteria.setFilterObjects(List.of(
                filterObject("NIFTY 50", "NIFTY", "1m"),
                filterObject("TCS", "TCS", "5m")
        ));

        SqlQuery query = HistoricalDataQueryBuilder.buildFind(criteria);

        assertThat(query.sql())
                .contains("WHERE (h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)"
                        + " OR (h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)")
                .contains("ORDER BY h.start_time DESC LIMIT " + HistoricalDataQueryBuilder.MAX_RESULTS)
                .contains("NULL::text AS quote", "NULL::text AS ltp", "NULL::text AS snap")
                .doesNotContain("LEFT JOIN app_info");
        assertThat(query.params()).containsExactly("NIFTY 50", "NIFTY", "1m", "TCS", "TCS", "5m");
    }

    @Test
    void buildFindLatestPerFilter_buildsDistinctOnDisjunction() {
        List<FilterCriteria.FilterObject> filterObjects = List.of(
                filterObject("NIFTY 50", "NIFTY", "1m"),
                filterObject("TCS", "TCS", "5m")
        );

        SqlQuery query = HistoricalDataQueryBuilder.buildFindLatestPerFilter(filterObjects);

        assertThat(query.sql())
                .contains("SELECT DISTINCT ON (h.stockname, h.stock_symbol, h.interval)")
                .contains("(h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)"
                        + " OR (h.stockname = ? AND h.stock_symbol = ? AND h.interval = ?)")
                .contains("ORDER BY h.stockname, h.stock_symbol, h.interval, h.start_time DESC")
                // Unlike buildFind: this path IS "give me the current state", so the current-value
                // app_info join is correct here and must be preserved.
                .contains("LEFT JOIN app_info i ON h.info_id = i.id");
        assertThat(query.params()).containsExactly("NIFTY 50", "NIFTY", "1m", "TCS", "TCS", "5m");
    }

    @Test
    void buildFind_withOversizedFilterList_throws() {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setStockNames(java.util.Collections.nCopies(
                HistoricalDataQueryBuilder.MAX_FILTER_LIST_SIZE + 1, "NIFTY 50"));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> HistoricalDataQueryBuilder.buildFind(criteria))
                .isInstanceOf(HistoricalDataQueryBuilder.FilterTooLargeException.class);
    }

    private static FilterCriteria.FilterObject filterObject(String name, String symbol, String interval) {
        FilterCriteria.FilterObject fo = new FilterCriteria.FilterObject();
        fo.setStockname(name);
        fo.setStockSymbol(symbol);
        fo.setInterval(interval);
        return fo;
    }
}
