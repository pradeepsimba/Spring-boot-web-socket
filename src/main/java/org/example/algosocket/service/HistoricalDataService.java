package org.example.algosocket.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.repository.HistoricalDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class HistoricalDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalDataService.class);

    private final HistoricalDataRepository repository;
    private final ObjectMapper objectMapper;

    public HistoricalDataService(HistoricalDataRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public String getHistoricalDataAsJson(FilterCriteria filterCriteria) {
        List<HistoricalData> data = repository.find(filterCriteria);
        LOGGER.info("Fetched {} records from the database.", data.size());
        return toJson(data);
    }

    /** Bootstraps a live-feed session with the current candle for each subscribed filter. */
    public String getLatestPerFilterAsJson(List<FilterCriteria.FilterObject> filterObjects) {
        List<HistoricalData> data = repository.findLatestPerFilter(filterObjects);
        LOGGER.info("Fetched {} latest-candle records for live-feed bootstrap.", data.size());
        return toJson(data);
    }

    private String toJson(List<HistoricalData> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error converting data to JSON", e);
            return "[]";
        }
    }
}
