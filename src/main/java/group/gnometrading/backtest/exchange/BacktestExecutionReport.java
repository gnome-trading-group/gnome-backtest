package group.gnometrading.backtest.exchange;

import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.OrderStatus;

public class BacktestExecutionReport {

    public final String clientOid;
    public final ExecType execType;
    public final OrderStatus orderStatus;
    public final long filledQty;
    public final long fillPrice;
    public final long cumulativeQty;
    public final long leavesQty;
    public final double fee;

    public long timestampEvent = -1;
    public long timestampRecv = -1;
    public int exchangeId = -1;
    public int securityId = -1;

    public BacktestExecutionReport(
            String clientOid,
            ExecType execType,
            OrderStatus orderStatus,
            long filledQty,
            long fillPrice,
            long cumulativeQty,
            long leavesQty,
            double fee) {
        this.clientOid = clientOid;
        this.execType = execType;
        this.orderStatus = orderStatus;
        this.filledQty = filledQty;
        this.fillPrice = fillPrice;
        this.cumulativeQty = cumulativeQty;
        this.leavesQty = leavesQty;
        this.fee = fee;
    }

    public static BacktestExecutionReport rejected(String clientOid) {
        return new BacktestExecutionReport(clientOid, ExecType.REJECT, OrderStatus.REJECTED, 0, 0, 0, 0, 0.0);
    }

    public static BacktestExecutionReport newOrder(String clientOid, long size) {
        return new BacktestExecutionReport(clientOid, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, size, 0.0);
    }

    public static BacktestExecutionReport canceled(String clientOid) {
        return new BacktestExecutionReport(clientOid, ExecType.CANCEL, OrderStatus.CANCELED, 0, 0, 0, 0, 0.0);
    }
}
