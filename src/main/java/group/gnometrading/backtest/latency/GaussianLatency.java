package group.gnometrading.backtest.latency;

import java.util.Random;

public final class GaussianLatency implements LatencyModel {

    private final double mu;
    private final double sigma;
    private final Random random;

    public GaussianLatency(double mu, double sigma) {
        this.mu = mu;
        this.sigma = sigma;
        this.random = new Random();
    }

    @Override
    public long simulate() {
        return (long) (mu + sigma * random.nextGaussian());
    }
}
