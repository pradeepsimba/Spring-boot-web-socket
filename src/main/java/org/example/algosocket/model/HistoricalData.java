package org.example.algosocket.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "app_historical_data")
public class HistoricalData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonProperty("stockname")
    private String stockname;
    @JsonProperty("stock_symbol")
    private String stockSymbol;
    @JsonProperty("interval")
    private String interval;
    @JsonProperty("start_time")
    private LocalDateTime startTime;
    @JsonProperty("open")
    private Double open;
    @JsonProperty("high")
    private Double high;
    @JsonProperty("low")
    private Double low;
    @JsonProperty("close")
    private Double close;
    @JsonProperty("volume")
    private Long volume;
    @JsonProperty("updated_at")
    private LocalDateTime updatedAt;

    public HistoricalData() {
    }

    private String quote;
    private String ltp;
    private String snap;

    public HistoricalData(String stockname, String stockSymbol, String interval, LocalDateTime startTime, Double open, Double high, Double low, Double close, Long volume, LocalDateTime updatedAt, String quote, String ltp, String snap) {
        this.stockname = stockname;
        this.stockSymbol = stockSymbol;
        this.interval = interval;
        this.startTime = startTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.updatedAt = updatedAt;
        this.quote = quote;
        this.ltp = ltp;
        this.snap = snap;
    }

    public String getQuote() { return quote; }
    public void setQuote(String quote) { this.quote = quote; }
    public String getLtp() { return ltp; }
    public void setLtp(String ltp) { this.ltp = ltp; }
    public String getSnap() { return snap; }
    public void setSnap(String snap) { this.snap = snap; }

    // Getters and setters

    public String getStockname() {
        return stockname;
    }

    public void setStockname(String stockname) {
        this.stockname = stockname;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public Double getOpen() {
        return open;
    }

    public void setOpen(Double open) {
        this.open = open;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }

    public Long getVolume() {
        return volume;
    }

    public void setVolume(Long volume) {
        this.volume = volume;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
