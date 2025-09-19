package org.example.algosocket.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.algosocket.model.FilterCriteria;
import org.example.algosocket.model.HistoricalData;
import org.example.algosocket.repository.HistoricalDataRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
public class HistoricalDataService {
    public List<HistoricalData> getLatestDataList(FilterCriteria filterCriteria) {
        return repository.find(filterCriteria);
    }
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    private static final Logger LOGGER = Logger.getLogger(HistoricalDataService.class.getName());

    private final HistoricalDataRepository repository;
    private final ObjectMapper objectMapper;
    private LocalDateTime lastUpdateTime;

    public HistoricalDataService(HistoricalDataRepository repository, ObjectMapper objectMapper) {
    this.repository = repository;
    this.objectMapper = objectMapper;
    LocalDateTime latest = repository.findLatestUpdateTime();
    this.lastUpdateTime = latest != null ? latest : LocalDateTime.now();
    }

    public String getAllHistoricalDataAsJson() {
        return getHistoricalDataAsJson(new FilterCriteria());
    }

    public String getHistoricalDataAsJson(FilterCriteria filterCriteria) {
        List<HistoricalData> data = repository.find(filterCriteria);
        LOGGER.info("Fetched " + data.size() + " records from the database.");
        updateLastUpdateTime(data);
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.SEVERE, "Error converting data to JSON", e);
            return "[]";
        }
    }

    public String getLatestDataAsJson(FilterCriteria filterCriteria) {
        List<HistoricalData> data = repository.findLatest(filterCriteria, lastUpdateTime);
        LOGGER.info("Fetched " + data.size() + " new records from the database.");
        updateLastUpdateTime(data);
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.SEVERE, "Error converting data to JSON", e);
            return "[]";
        }
    }

    private void updateLastUpdateTime(List<HistoricalData> data) {
        if (data != null && !data.isEmpty()) {
            data.stream()
                    .map(HistoricalData::getUpdatedAt)
                    .filter(java.util.Objects::nonNull)
                    .max(LocalDateTime::compareTo)
                    .ifPresent(latest -> lastUpdateTime = latest);
        }
    }
}