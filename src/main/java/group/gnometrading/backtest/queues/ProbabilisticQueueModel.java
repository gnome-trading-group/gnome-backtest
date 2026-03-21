package group.gnometrading.backtest.queues;

import group.gnometrading.backtest.book.LocalOrder;

import java.util.ArrayDeque;

/**
 * Interpolates between optimistic and risk-averse based on cancelAheadProbability.
 * When depth decreases by D, phantom is reduced by round(p * D) where p in [0, 1].
 * Depth increases are assumed to arrive behind us.
 */
public class ProbabilisticQueueModel implements QueueModel {

    private final double cancelAheadProbability;

    public ProbabilisticQueueModel(double cancelAheadProbability) {
        if (cancelAheadProbability < 0 || cancelAheadProbability > 1) {
            throw new IllegalArgumentException("cancelAheadProbability must be in [0, 1]");
        }
        this.cancelAheadProbability = cancelAheadProbability;
    }

    @Override
    public void onModify(long previousQuantity, long newQuantity, ArrayDeque<LocalOrder> localQueue) {
        if (localQueue.isEmpty()) {
            return;
        }
        long removedVolume = previousQuantity - newQuantity;
        if (removedVolume <= 0) {
            return;
        }
        long expectedAhead = Math.round(cancelAheadProbability * removedVolume);
        if (expectedAhead <= 0) {
            return;
        }
        for (LocalOrder localOrder : localQueue) {
            localOrder.phantomVolume = Math.max(0, localOrder.phantomVolume - expectedAhead);
        }
    }
}
