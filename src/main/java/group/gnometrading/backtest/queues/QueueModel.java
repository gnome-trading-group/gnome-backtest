package group.gnometrading.backtest.queues;

import group.gnometrading.backtest.book.LocalOrder;
import group.gnometrading.backtest.book.LocalOrderFill;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public interface QueueModel {

    void onModify(long previousQuantity, long newQuantity, ArrayDeque<LocalOrder> localQueue);

    /**
     * Allocates a trade across local orders in the queue based on phantom volume and remaining quantity.
     * Phantom volume is consumed first; fills only occur once phantom is exhausted.
     */
    default List<LocalOrderFill> onTrade(long tradeSize, ArrayDeque<LocalOrder> localQueue) {
        long phantomVolumeConsumed = 0;
        boolean seenFirst = false;

        for (LocalOrder localOrder : localQueue) {
            if (!seenFirst) {
                phantomVolumeConsumed = Math.max(0, localOrder.phantomVolume);
                seenFirst = true;
            }
            localOrder.phantomVolume -= tradeSize;
            localOrder.cumulativeTradedQuantity += tradeSize;
        }

        List<LocalOrderFill> filledOrders = new ArrayList<>();
        long remainingVolume = tradeSize - (seenFirst ? phantomVolumeConsumed : 0);

        while (!localQueue.isEmpty() && remainingVolume > 0) {
            LocalOrder localOrder = localQueue.peek();
            if (localOrder.phantomVolume <= 0) {
                long filledQty = Math.min(localOrder.remaining, remainingVolume);
                remainingVolume -= filledQty;
                localOrder.remaining -= filledQty;
                filledOrders.add(new LocalOrderFill(localOrder, filledQty));
                if (localOrder.remaining == 0) {
                    localQueue.poll();
                }
            } else {
                break;
            }
        }

        return filledOrders;
    }
}
