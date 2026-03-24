package group.gnometrading.backtest.driver;

public final class BacktestEvent implements Comparable<BacktestEvent> {

    public final long timestamp;
    public final EventType eventType;
    public final Object data;

    public BacktestEvent(long timestamp, EventType eventType, Object data) {
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.data = data;
    }

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
