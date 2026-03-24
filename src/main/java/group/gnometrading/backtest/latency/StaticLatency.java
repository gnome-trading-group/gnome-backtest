package group.gnometrading.backtest.latency;

public final class StaticLatency implements LatencyModel {

    private final long latencyNanos;

    public StaticLatency(long latencyNanos) {
        this.latencyNanos = latencyNanos;
    }

    @Override
    public long simulate() {
        return latencyNanos;
    }
}
