package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.schemas.Schema;
import java.util.List;

public interface BacktestStrategy {

    /**
     * Called when the strategy receives a market data update.
     * Returns local messages (orders/cancels) to submit to the exchange.
     */
    List<LocalMessage> onMarketData(long timestamp, Schema data);

    /**
     * Called when the strategy receives an execution report from the exchange.
     * Returns local messages (orders/cancels) triggered by the report (e.g., queued order replacements).
     */
    List<LocalMessage> onExecutionReport(long timestamp, BacktestExecutionReport report);

    /** Returns simulated strategy processing time in nanoseconds. */
    long simulateProcessingTime();
}
