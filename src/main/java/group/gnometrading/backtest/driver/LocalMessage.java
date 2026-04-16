package group.gnometrading.backtest.driver;

import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;

public sealed interface LocalMessage
        permits LocalMessage.OrderMessage, LocalMessage.CancelOrderMessage, LocalMessage.ModifyOrderMessage {

    int exchangeId();

    int securityId();

    record OrderMessage(Order order) implements LocalMessage {
        public int exchangeId() {
            return (int) order.decoder.exchangeId();
        }

        public int securityId() {
            return (int) order.decoder.securityId();
        }
    }

    record CancelOrderMessage(CancelOrder cancelOrder) implements LocalMessage {
        public int exchangeId() {
            return (int) cancelOrder.decoder.exchangeId();
        }

        public int securityId() {
            return (int) cancelOrder.decoder.securityId();
        }
    }

    record ModifyOrderMessage(ModifyOrder modifyOrder) implements LocalMessage {
        public int exchangeId() {
            return (int) modifyOrder.decoder.exchangeId();
        }

        public int securityId() {
            return (int) modifyOrder.decoder.securityId();
        }
    }
}
