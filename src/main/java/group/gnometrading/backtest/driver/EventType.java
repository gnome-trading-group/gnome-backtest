package group.gnometrading.backtest.driver;

public enum EventType {
    /** When a simulated exchange processes a market update (applies trades/fills). */
    EXCHANGE_MARKET_DATA(0),
    /** When a simulated exchange processes messages whose processing time is up (matching happens here). */
    EXCHANGE_PROCESS(1),
    /** Messages sent from the exchange to the local strategy (execution reports). */
    EXCHANGE_MESSAGE(2),
    /** When the local strategy processes a market update. */
    LOCAL_MARKET_DATA(3),
    /** Messages from the strategy arriving at the exchange (orders, cancels, modifies). */
    LOCAL_MESSAGE(4),
    /** When the strategy finishes processing an event: its intents reach the OMS and it can take the next one. */
    STRATEGY_DONE(5);

    public final int value;

    EventType(int value) {
        this.value = value;
    }
}
