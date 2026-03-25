package group.gnometrading.backtest.oms;

import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.intent.Intent;
import group.gnometrading.oms.intent.OmsAction;
import group.gnometrading.oms.order.OmsCancelOrder;
import group.gnometrading.oms.order.OmsExecutionReport;
import group.gnometrading.oms.order.OmsOrder;
import group.gnometrading.oms.order.OmsReplaceOrder;
import group.gnometrading.oms.state.TrackedOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Bridges the OMS intent system to BacktestDriver's LocalMessage format.
 *
 * Handles both directions:
 *   - Intents → OMS → approved actions → LocalMessage (for simulated exchange)
 *   - BacktestExecutionReport → OmsExecutionReport (for OMS fill processing)
 *
 * Optionally records intents and execution reports to a BacktestRecorder.
 */
public final class OmsBacktestAdapter {

    private final OrderManagementSystem oms;
    private final BacktestRecorder recorder;
    private final List<LocalMessage> buffer = new ArrayList<>();
    private final OmsExecutionReport omsReport = new OmsExecutionReport();

    public OmsBacktestAdapter(OrderManagementSystem oms) {
        this(oms, null);
    }

    public OmsBacktestAdapter(OrderManagementSystem oms, BacktestRecorder recorder) {
        this.oms = oms;
        this.recorder = recorder;
        this.oms.setActionConsumer(this::onAction);
    }

    public List<LocalMessage> processIntents(long timestamp, Intent[] intents, int count) {
        buffer.clear();
        for (int i = 0; i < count; i++) {
            if (recorder != null) {
                recorder.onIntent(timestamp, intents[i]);
            }
            oms.processIntent(intents[i]);
        }
        return buffer;
    }

    public List<LocalMessage> processIntent(long timestamp, Intent intent) {
        buffer.clear();
        if (recorder != null) {
            recorder.onIntent(timestamp, intent);
        }
        oms.processIntent(intent);
        return buffer;
    }

    public void processExecutionReport(BacktestExecutionReport report) {
        processExecutionReport(report, 0);
    }

    public void processExecutionReport(BacktestExecutionReport report, int strategyId) {
        if (recorder != null) {
            // Look up original order price/size from OMS
            long orderPrice = 0;
            long orderSize = 0;
            TrackedOrder tracked = oms.getOrder(Long.parseLong(report.clientOid));
            if (tracked != null) {
                orderPrice = tracked.getPrice();
                orderSize = tracked.getSize();
            }
            recorder.onExecutionReport(report.timestampRecv, report, strategyId, orderPrice, orderSize);
        }
        omsReport.set(
                Long.parseLong(report.clientOid),
                strategyId,
                report.execType,
                report.orderStatus,
                report.filledQty,
                report.fillPrice,
                report.cumulativeQty,
                report.leavesQty,
                report.fee,
                report.exchangeId,
                report.securityId,
                report.timestampEvent,
                report.timestampRecv);
        oms.processExecutionReport(omsReport);
    }

    private void onAction(OmsAction action) {
        switch (action.type()) {
            case NEW_ORDER -> {
                OmsOrder order = action.order();
                buffer.add(new LocalMessage.OrderMessage(new BacktestOrder(
                        order.exchangeId(),
                        (int) order.securityId(),
                        String.valueOf(order.clientOid()),
                        order.side(),
                        order.price(),
                        order.size(),
                        order.orderType(),
                        order.timeInForce())));
            }
            case REPLACE -> {
                OmsReplaceOrder rep = action.replace();
                buffer.add(new LocalMessage.CancelOrderMessage(new BacktestCancelOrder(
                        rep.exchangeId(), (int) rep.securityId(), String.valueOf(rep.originalClientOid()))));
                TrackedOrder tracked = oms.getOrder(rep.originalClientOid());
                if (tracked != null) {
                    buffer.add(new LocalMessage.OrderMessage(new BacktestOrder(
                            rep.exchangeId(),
                            (int) rep.securityId(),
                            String.valueOf(rep.newClientOid()),
                            tracked.getSide(),
                            rep.price(),
                            rep.size(),
                            tracked.getOrderType(),
                            tracked.getTimeInForce())));
                }
            }
            case CANCEL -> {
                OmsCancelOrder cancel = action.cancel();
                buffer.add(new LocalMessage.CancelOrderMessage(new BacktestCancelOrder(
                        cancel.exchangeId(), (int) cancel.securityId(), String.valueOf(cancel.clientOid()))));
            }
        }
    }

    public OrderManagementSystem getOms() {
        return oms;
    }
}
