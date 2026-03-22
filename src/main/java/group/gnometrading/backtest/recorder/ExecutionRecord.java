package group.gnometrading.backtest.recorder;

public record ExecutionRecord(
        long timestampEvent,
        long timestampRecv,
        int exchangeId,
        int securityId,
        String clientOid,
        String execType,
        String orderStatus,
        long filledQty,
        long fillPrice,
        long cumulativeQty,
        long leavesQty,
        double fee
) {}
