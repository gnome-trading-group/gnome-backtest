package group.gnometrading.backtest.driver;

import org.agrona.concurrent.EpochNanoClock;

/**
 * The backtest's notion of now, advanced by the driver as it replays events. Everything that reads a clock in a
 * backtest reads this one; wall time would make runs nondeterministic.
 */
public final class SimulatedClock implements EpochNanoClock {

    private long nowNanos;

    @Override
    public long nanoTime() {
        return nowNanos;
    }

    public void set(long nanos) {
        this.nowNanos = nanos;
    }
}
