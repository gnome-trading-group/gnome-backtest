package group.gnometrading.backtest.driver;

/**
 * An event in the backtest queue.
 *
 * <p>{@code sequence} is the order the event was enqueued. PriorityQueue does not keep insertion order among equal
 * keys, so without it, records sharing a timestamp (and the reports one exchange action emits together) could be
 * processed in any order.
 */
public record BacktestEvent(long timestamp, EventType eventType, long sequence, Object data)
        implements Comparable<BacktestEvent> {

    @Override
    public int compareTo(BacktestEvent other) {
        int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp != 0) {
            return cmp;
        }
        // Secondary: EXCHANGE_MARKET_DATA < EXCHANGE_PROCESS < EXCHANGE_MESSAGE < LOCAL_MARKET_DATA < LOCAL_MESSAGE
        // < STRATEGY_DONE
        cmp = Integer.compare(this.eventType.value, other.eventType.value);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.sequence, other.sequence);
    }
}
