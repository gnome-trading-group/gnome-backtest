package group.gnometrading.backtest.recorder;

/**
 * Write-only handle for strategy custom metric recording.
 *
 * <p>Strategies call {@link #createBuffer} during their {@code setMetricRecorder} / {@code
 * register_metrics} phase to declare named record streams. The returned {@link RecordBuffer}
 * is used directly on the hot path: {@code buf.appendRow()} + {@code buf.setDouble(...)}.
 *
 * <p>Strategies never hold a reference to {@link BacktestRecorder} directly; this facade
 * ensures they can only add data, not read back built-in or other strategy streams.
 */
public final class MetricRecorder {

    private static final int DEFAULT_INITIAL_CAPACITY = 1_000_000;

    private final BacktestRecorder recorder;

    MetricRecorder(BacktestRecorder recorder) {
        this.recorder = recorder;
    }

    /**
     * Creates a new named record stream backed by a fresh {@link RecordBuffer}.
     *
     * <p>Callers must add columns and call {@link RecordBuffer#freeze()} before the backtest starts.
     *
     * @param name unique name for this stream; used as the parquet file name and DataFrame key
     * @param initialCapacity initial row capacity (grows by 2× automatically)
     */
    public RecordBuffer createBuffer(String name, int initialCapacity) {
        return recorder.createBuffer(name, initialCapacity);
    }

    /**
     * Creates a new named record stream with the default initial capacity (1,000,000 rows).
     */
    public RecordBuffer createBuffer(String name) {
        return createBuffer(name, DEFAULT_INITIAL_CAPACITY);
    }
}
