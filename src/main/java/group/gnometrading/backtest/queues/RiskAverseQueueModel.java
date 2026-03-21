package group.gnometrading.backtest.queues;

import group.gnometrading.backtest.book.LocalOrder;

import java.util.ArrayDeque;

/**
 * Assumes all cancels happen behind our position and all trades happen in front.
 * Phantom is clamped to min(phantom, newQuantity) on any depth decrease.
 */
public class RiskAverseQueueModel implements QueueModel {

    @Override
    public void onModify(long previousQuantity, long newQuantity, ArrayDeque<LocalOrder> localQueue) {
        for (LocalOrder localOrder : localQueue) {
            localOrder.phantomVolume = Math.min(localOrder.phantomVolume, newQuantity);
        }
    }
}
