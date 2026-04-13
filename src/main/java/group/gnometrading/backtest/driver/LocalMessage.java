package group.gnometrading.backtest.driver;

import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;

public sealed interface LocalMessage
        permits LocalMessage.OrderMessage, LocalMessage.CancelOrderMessage, LocalMessage.ModifyOrderMessage {

    record OrderMessage(Order order) implements LocalMessage {}

    record CancelOrderMessage(CancelOrder cancelOrder) implements LocalMessage {}

    record ModifyOrderMessage(ModifyOrder modifyOrder) implements LocalMessage {}
}
