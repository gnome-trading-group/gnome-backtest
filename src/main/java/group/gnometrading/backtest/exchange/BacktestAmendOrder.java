package group.gnometrading.backtest.exchange;

public record BacktestAmendOrder(int exchangeId, int securityId, String clientOid, long newPrice, long newSize) {}
