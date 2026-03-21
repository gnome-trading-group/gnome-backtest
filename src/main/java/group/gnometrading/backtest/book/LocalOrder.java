package group.gnometrading.backtest.book;

import group.gnometrading.backtest.exchange.BacktestOrder;

public class LocalOrder {

    public final BacktestOrder order;
    public long remaining;
    public long phantomVolume;
    public long cumulativeTradedQuantity;

    public LocalOrder(BacktestOrder order, long remaining, long phantomVolume) {
        this.order = order;
        this.remaining = remaining;
        this.phantomVolume = phantomVolume;
        this.cumulativeTradedQuantity = 0;
    }
}
