package org.example.algosocket.model;

import java.time.LocalDateTime;
import java.util.List;

public class FilterCriteria {

    private LocalDateTime fromTime;
    private LocalDateTime toTime;
    private List<String> stockNames;

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
}
