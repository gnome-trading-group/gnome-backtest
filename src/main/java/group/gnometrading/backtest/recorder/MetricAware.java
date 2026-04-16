package group.gnometrading.backtest.recorder;

/**
 * Implemented by Java strategy classes that want to record custom metrics during a backtest.
 *
 * <p>{@link group.gnometrading.backtest.driver.BacktestDriver} checks
 * {@code strategy instanceof MetricAware} and calls {@link #setMetricRecorder} before replay
 * starts. Strategies use the recorder to create named {@link RecordBuffer}s and register columns.
 *
 * <p>Note: this interface lives in gnome-backtest, not gnome-strategies, so production
 * strategy code does not need a backtest dependency.
 */
public interface MetricAware {

    /**
     * Called by the backtest driver before replay starts.
     *
     * <p>Implementations should create one or more {@link RecordBuffer}s, add columns,
     * and call {@link RecordBuffer#freeze()} on each before this method returns.
     */
    void setMetricRecorder(MetricRecorder recorder);
}
