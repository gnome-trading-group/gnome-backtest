package group.gnometrading.backtest.exchange;

import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import java.util.List;

public interface SimulatedExchange {

    List<OrderExecutionReport> submitOrder(Order order);

    List<OrderExecutionReport> cancelOrder(CancelOrder cancel);

    List<OrderExecutionReport> modifyOrder(ModifyOrder modify);

    List<OrderExecutionReport> onMarketData(Schema data);

    long simulateNetworkLatency();

    long simulateOrderProcessingTime();

    List<SchemaType> getSupportedSchemas();
}
