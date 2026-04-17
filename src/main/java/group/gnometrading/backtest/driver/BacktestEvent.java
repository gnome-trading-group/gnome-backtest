package group.gnometrading.backtest.driver;

public record BacktestEvent(long timestamp, EventType eventType, Object data) implements Comparable<BacktestEvent> {

    @Override
    public int compareTo(BacktestEvent other) {
        int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp != 0) {
            return cmp;
        }
        // Secondary: EXCHANGE_MARKET_DATA < EXCHANGE_MESSAGE < LOCAL_MARKET_DATA < LOCAL_MESSAGE
        return Integer.compare(this.eventType.value, other.eventType.value);
    }
}
