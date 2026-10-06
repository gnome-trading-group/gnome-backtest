package group.gnometrading.backtest.driver;

/**
 * One connection between us and a venue. Like TCP, it delivers in the order messages were sent: each message's
 * latency is drawn on its own, but it can't arrive before one sent ahead of it, so a slow message holds up the rest.
 */
final class NetworkChannel {

    private long lastArrival = Long.MIN_VALUE;

    long arrival(long sentNanos, long latencyNanos) {
        lastArrival = Math.max(sentNanos + latencyNanos, lastArrival);
        return lastArrival;
    }
}
