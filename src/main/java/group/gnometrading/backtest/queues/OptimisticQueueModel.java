package group.gnometrading.backtest.queues;

import group.gnometrading.backtest.book.LocalOrder;
import java.util.ArrayDeque;

/**
 * Assumes cancels occur ahead of our position (reducing phantom), and new orders arrive behind us.
 * When displayed depth decreases, phantom is reduced by the removed volume (floored at zero).
 * When depth increases, phantom is unchanged.
 */
public final class OptimisticQueueModel implements QueueModel {

    @Override
    public void onModify(long previousQuantity, long newQuantity, ArrayDeque<LocalOrder> localQueue) {
        if (localQueue.isEmpty()) {
            return;
        }
        long removedVolume = previousQuantity - newQuantity;
        if (removedVolume <= 0) {
            return;
        }
        for (LocalOrder localOrder : localQueue) {
            localOrder.phantomVolume = Math.max(0, localOrder.phantomVolume - removedVolume);
        }
    }
}
