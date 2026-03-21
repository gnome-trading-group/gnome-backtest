package group.gnometrading.backtest.exchange;

public record BacktestCancelOrder(int exchangeId, int securityId, String clientOid) {}
