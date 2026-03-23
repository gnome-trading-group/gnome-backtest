package group.gnometrading.backtest.oms;

import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.intent.Intent;
import group.gnometrading.oms.intent.OmsAction;
import group.gnometrading.oms.order.OmsCancelOrder;
import group.gnometrading.oms.order.OmsOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Bridges the OMS intent system to BacktestDriver's LocalMessage format.
 *
 * Takes intents, runs them through the OMS (resolve → validate → track),
 * and converts approved actions to LocalMessage objects for the BacktestDriver.
 * All conversion happens in Java — no Python callbacks needed.
 */
public class OmsBacktestAdapter {

    private final OrderManagementSystem oms;
    private final List<LocalMessage> buffer = new ArrayList<>();

    public OmsBacktestAdapter(OrderManagementSystem oms) {
        this.oms = oms;
    }

    public List<LocalMessage> processIntents(Intent[] intents, int count) {
        buffer.clear();
        for (int i = 0; i < count; i++) {
            oms.processIntent(intents[i], this::onAction);
        }
        return buffer;
    }

    public List<LocalMessage> processIntent(Intent intent) {
        buffer.clear();
        oms.processIntent(intent, this::onAction);
        return buffer;
    }

    public void processExecutionReport(BacktestExecutionReport report) {
        oms.processExecutionReport(BacktestOmsAdapter.toOmsReport(report));
    }

    private void onAction(OmsAction action) {
        if (action instanceof OmsAction.NewOrder newOrder) {
            OmsOrder order = newOrder.order();
            BacktestOrder btOrder = new BacktestOrder(
                    order.exchangeId(),
                    (int) order.securityId(),
                    order.clientOid(),
                    order.side(),
                    order.price(),
                    order.size(),
                    order.orderType(),
                    order.timeInForce()
            );
            buffer.add(new LocalMessage.OrderMessage(btOrder));
        } else if (action instanceof OmsAction.Cancel cancel) {
            OmsCancelOrder c = cancel.cancel();
            BacktestCancelOrder btCancel = new BacktestCancelOrder(
                    c.exchangeId(),
                    (int) c.securityId(),
                    c.clientOid()
            );
            buffer.add(new LocalMessage.CancelOrderMessage(btCancel));
        }
    }

    public OrderManagementSystem getOms() {
        return oms;
    }
}
