package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestOrder;

public sealed interface LocalMessage permits LocalMessage.OrderMessage, LocalMessage.CancelOrderMessage {

    record OrderMessage(BacktestOrder order) implements LocalMessage {}

    record CancelOrderMessage(BacktestCancelOrder cancelOrder) implements LocalMessage {}
}
