package group.gnometrading.backtest.exchange;

import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import java.util.List;

public interface SimulatedExchange {

    List<BacktestExecutionReport> submitOrder(BacktestOrder order);

    List<BacktestExecutionReport> cancelOrder(BacktestCancelOrder cancel);

    List<BacktestExecutionReport> onMarketData(Schema data);

    long simulateNetworkLatency();

    long simulateOrderProcessingTime();

    List<SchemaType> getSupportedSchemas();
}
