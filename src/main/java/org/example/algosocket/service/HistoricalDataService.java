package org.example.algosocket.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.repository.HistoricalDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class HistoricalDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalDataService.class);

    // How long a "latest candle" bootstrap result may be reused for another client subscribing to
    // the exact same (stockname, stock_symbol, interval) shortly after. Deliberately short:
    // quote/ltp/snap (joined in from app_info) can change on every single tick, far more often
    // than this - this cache only exists to collapse a burst of near-simultaneous subscribes to
    // the same filter (e.g. many clients reconnecting after a network blip) into one DB query
    // instead of one per client. It's a one-time snapshot at connect time either way - the live
    // feed itself takes over immediately after with the true current value regardless of whether
    // this snapshot came from cache or a fresh query.
    private static final long LATEST_CACHE_TTL_NANOS = TimeUnit.MILLISECONDS.toNanos(300);

    private final HistoricalDataRepository repository;
    private final ObjectMapper objectMapper;
    private final Map<CacheKey, CachedLatest> latestCache = new ConcurrentHashMap<>();

    public HistoricalDataService(HistoricalDataRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public String getHistoricalDataAsJson(FilterCriteria filterCriteria) {
        List<HistoricalData> data = repository.find(filterCriteria);
        LOGGER.debug("Fetched {} records from the database.", data.size());
        return toJson(data);
    }

    /** Bootstraps a live-feed session with the current candle for each subscribed filter. */
    public String getLatestPerFilterAsJson(List<FilterCriteria.FilterObject> filterObjects) {
        // Distinct by key first: a client's filter list could contain the same
        // (stockname, stock_symbol, interval) twice - buildFindLatestPerFilter's DISTINCT ON
        // already collapsed that at the SQL level, but doing it here too preserves that same
        // guarantee once some keys are served from cache and others aren't.
        Map<CacheKey, FilterCriteria.FilterObject> distinct = new LinkedHashMap<>();
        for (FilterCriteria.FilterObject fo : filterObjects) {
            distinct.putIfAbsent(CacheKey.of(fo), fo);
        }

        long now = System.nanoTime();
        List<HistoricalData> result = new ArrayList<>(distinct.size());
        List<FilterCriteria.FilterObject> misses = new ArrayList<>();

        for (Map.Entry<CacheKey, FilterCriteria.FilterObject> entry : distinct.entrySet()) {
            CachedLatest cached = latestCache.get(entry.getKey());
            if (cached != null && (now - cached.fetchedAtNanos()) < LATEST_CACHE_TTL_NANOS) {
                result.add(cached.data());
            } else {
                misses.add(entry.getValue());
            }
        }

        if (!misses.isEmpty()) {
            List<HistoricalData> fetched = repository.findLatestPerFilter(misses);
            for (HistoricalData data : fetched) {
                latestCache.put(CacheKey.of(data), new CachedLatest(data, now));
                result.add(data);
            }
        }

        LOGGER.debug("Latest-per-filter: {} served from cache, {} fetched from DB.",
                distinct.size() - misses.size(), misses.size());
        return toJson(result);
    }

    private String toJson(List<HistoricalData> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error converting data to JSON", e);
            return "[]";
        }
    }

    /** Keyed the same way the live feed groups ticks - see HistoricalDataWebSocketHandler.FilterKey. */
    private record CacheKey(String stockname, String stockSymbol, String interval) {
        static CacheKey of(FilterCriteria.FilterObject fo) {
            return new CacheKey(normalize(fo.getStockname()), normalize(fo.getStockSymbol()), fo.getInterval());
        }
        static CacheKey of(HistoricalData d) {
            return new CacheKey(normalize(d.getStockname()), normalize(d.getStockSymbol()), d.getInterval());
        }
        private static String normalize(String s) {
            return s == null ? null : s.toUpperCase(Locale.ROOT);
        }
    }

    private record CachedLatest(HistoricalData data, long fetchedAtNanos) {
    }
}
