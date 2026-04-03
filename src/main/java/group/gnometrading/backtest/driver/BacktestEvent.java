package group.gnometrading.backtest.driver;

public record BacktestEvent(long timestamp, EventType eventType, Object data) implements Comparable<BacktestEvent> {

    @Override
    public int compareTo(BacktestEvent other) {
        int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp != 0) {
            return cmp;
        }
        // Secondary ordering: EXCHANGE_MARKET_DATA before LOCAL_MARKET_DATA at same timestamp
        return Integer.compare(this.eventType.value, other.eventType.value);
    }
}
