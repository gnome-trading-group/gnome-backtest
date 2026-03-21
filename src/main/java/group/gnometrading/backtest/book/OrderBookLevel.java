package group.gnometrading.backtest.book;

import java.util.ArrayDeque;

public class OrderBookLevel {

    public final long price;
    public long size;
    public final ArrayDeque<LocalOrder> localOrders;

    public OrderBookLevel(long price, long size) {
        this.price = price;
        this.size = size;
        this.localOrders = new ArrayDeque<>();
    }

    public boolean hasLocalOrders() {
        return !localOrders.isEmpty();
    }
}
