package group.gnometrading.backtest.recorder;

public record MarketRecord(
        long timestamp,
        int exchangeId,
        long securityId,
        long bestBidPrice,
        long bestAskPrice,
        long bestBidSize,
        long bestAskSize,
        long lastTradePrice,
        long lastTradeSize,
        long sequenceNumber
) {}
