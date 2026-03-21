package group.gnometrading.backtest.latency;

public interface LatencyModel {
    /** Returns simulated latency in nanoseconds. */
    long simulate();
}
