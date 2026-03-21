package group.gnometrading.backtest.driver;

public enum EventType {
    /** When a simulated exchange processes a market update (applies trades/fills). */
    EXCHANGE_MARKET_DATA(0),
    /** Messages sent from the exchange to the local strategy (execution reports). */
    EXCHANGE_MESSAGE(1),
    /** When the local strategy processes a market update. */
    LOCAL_MARKET_DATA(2),
    /** Messages sent from the strategy to the exchange (orders, cancels). */
    LOCAL_MESSAGE(3);

    public final int value;

    EventType(int value) {
        this.value = value;
    }
}
