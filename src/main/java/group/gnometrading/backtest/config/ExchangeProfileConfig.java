package group.gnometrading.backtest.config;

import group.gnometrading.backtest.exchange.MbpSimulatedExchange;
import group.gnometrading.backtest.exchange.SimulatedExchange;

public final class ExchangeProfileConfig {

    public FeeModelConfig feeModel = new FeeModelConfig.Static();
    public LatencyConfig networkLatency = new LatencyConfig.Static();
    public LatencyConfig orderProcessingLatency = new LatencyConfig.Static();
    public QueueModelConfig queueModel = new QueueModelConfig.RiskAverse();

    public SimulatedExchange toSimulatedExchange() {
        return new MbpSimulatedExchange(
                feeModel.toModel(), networkLatency.toModel(), orderProcessingLatency.toModel(), queueModel.toModel());
    }
}
