package group.gnometrading.backtest.oms;

import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.oms.order.OmsCancelOrder;
import group.gnometrading.oms.order.OmsExecutionReport;
import group.gnometrading.oms.order.OmsOrder;

public final class BacktestOmsAdapter {

    private BacktestOmsAdapter() {}

    public static OmsOrder toOmsOrder(BacktestOrder bo) {
        return new OmsOrder(
                bo.exchangeId(),
                bo.securityId(),
                bo.clientOid(),
                bo.side(),
                bo.price(),
                bo.size(),
                bo.orderType(),
                bo.timeInForce()
        );
    }

    public static OmsCancelOrder toOmsCancelOrder(BacktestCancelOrder bc) {
        return new OmsCancelOrder(
                bc.exchangeId(),
                bc.securityId(),
                bc.clientOid()
        );
    }

    public static OmsExecutionReport toOmsReport(BacktestExecutionReport ber) {
        return new OmsExecutionReport(
                ber.clientOid,
                ber.execType,
                ber.orderStatus,
                ber.filledQty,
                ber.fillPrice,
                ber.cumulativeQty,
                ber.leavesQty,
                ber.fee,
                ber.exchangeId,
                ber.securityId,
                ber.timestampEvent,
                ber.timestampRecv
        );
    }
}
