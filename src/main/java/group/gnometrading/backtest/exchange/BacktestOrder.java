package group.gnometrading.backtest.exchange;

import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;

public record BacktestOrder(
        int exchangeId,
        int securityId,
        String clientOid,
        Side side,
        long price,
        long size,
        OrderType orderType,
        TimeInForce timeInForce) {}
