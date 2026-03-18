package org.example.algosocket.model;

import java.time.LocalDateTime;

public class HistoricalData {

    private Long id;
    private String stockname;
    private String stockSymbol;
    private String interval;
    private LocalDateTime startTime;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Long volume;
    private LocalDateTime updatedAt;
    private String quote;
    private String ltp;
    private String snap;

    public HistoricalData() {
    }

    public HistoricalData(String stockname, String stockSymbol, String interval, LocalDateTime startTime,
                          Double open, Double high, Double low, Double close, Long volume,
                          LocalDateTime updatedAt, String quote, String ltp, String snap) {
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

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getStockname() { return stockname; }
    public void setStockname(String stockname) { this.stockname = stockname; }

    public String getStockSymbol() { return stockSymbol; }
    public void setStockSymbol(String stockSymbol) { this.stockSymbol = stockSymbol; }

    public String getInterval() { return interval; }
    public void setInterval(String interval) { this.interval = interval; }

    public LocalDateTime getStartTime() { return startTime; }
    public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }

    public Double getOpen() { return open; }
    public void setOpen(Double open) { this.open = open; }

    public Double getHigh() { return high; }
    public void setHigh(Double high) { this.high = high; }

    public Double getLow() { return low; }
    public void setLow(Double low) { this.low = low; }

    public Double getClose() { return close; }
    public void setClose(Double close) { this.close = close; }

    public Long getVolume() { return volume; }
    public void setVolume(Long volume) { this.volume = volume; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public String getQuote() { return quote; }
    public void setQuote(String quote) { this.quote = quote; }

    public String getLtp() { return ltp; }
    public void setLtp(String ltp) { this.ltp = ltp; }

    public String getSnap() { return snap; }
    public void setSnap(String snap) { this.snap = snap; }
}
