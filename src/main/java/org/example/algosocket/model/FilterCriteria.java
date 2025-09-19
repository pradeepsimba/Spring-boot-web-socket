package org.example.algosocket.model;

import java.time.LocalDateTime;
import java.util.List;

public class FilterCriteria {
    public static class FilterObject {
        private String stockname;
        private String stockSymbol;
        private String interval;

        public String getStockname() { return stockname; }
        public void setStockname(String stockname) { this.stockname = stockname; }
        public String getStockSymbol() { return stockSymbol; }
        public void setStockSymbol(String stockSymbol) { this.stockSymbol = stockSymbol; }
        public String getInterval() { return interval; }
        public void setInterval(String interval) { this.interval = interval; }
    }

    private List<FilterObject> filterObjects;

    public List<FilterObject> getFilterObjects() { return filterObjects; }
    public void setFilterObjects(List<FilterObject> filterObjects) { this.filterObjects = filterObjects; }

    private LocalDateTime fromTime;
    private LocalDateTime toTime;
    private List<String> stockNames;
    private List<String> stockSymbols;
    private List<String> intervals;

    public LocalDateTime getFromTime() {
        return fromTime;
    }

    public void setFromTime(LocalDateTime fromTime) {
        this.fromTime = fromTime;
    }

    public LocalDateTime getToTime() {
        return toTime;
    }

    public void setToTime(LocalDateTime toTime) {
        this.toTime = toTime;
    }

    public List<String> getStockNames() {
        return stockNames;
    }

    public void setStockNames(List<String> stockNames) {
        this.stockNames = stockNames;
    }

    public List<String> getStockSymbols() {
        return stockSymbols;
    }

    public void setStockSymbols(List<String> stockSymbols) {
        this.stockSymbols = stockSymbols;
    }

    public List<String> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<String> intervals) {
        this.intervals = intervals;
    }
}
