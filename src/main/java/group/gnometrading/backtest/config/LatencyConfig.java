package group.gnometrading.backtest.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import group.gnometrading.backtest.latency.GaussianLatency;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.latency.StaticLatency;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LatencyConfig.Static.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = LatencyConfig.Static.class, name = "static"),
    @JsonSubTypes.Type(value = LatencyConfig.Gaussian.class, name = "gaussian")
})
public abstract class LatencyConfig {

    public abstract LatencyModel toModel();

    public static final class Static extends LatencyConfig {
        public long latencyNanos;

        @Override
        public LatencyModel toModel() {
            return new StaticLatency(latencyNanos);
        }
    }

    public static final class Gaussian extends LatencyConfig {
        public double mu;
        public double sigma;

        @Override
        public LatencyModel toModel() {
            return new GaussianLatency(mu, sigma);
        }
    }
}
