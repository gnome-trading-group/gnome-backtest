package group.gnometrading.backtest.book;

import group.gnometrading.schemas.Order;

public class LocalOrder {

    public Order order;
    public long remaining;
    public long phantomVolume;
    public long cumulativeTradedQuantity;

    public LocalOrder(Order order, long remaining, long phantomVolume) {
        this.order = order;
        this.remaining = remaining;
        this.phantomVolume = phantomVolume;
        this.cumulativeTradedQuantity = 0;
    }
}
