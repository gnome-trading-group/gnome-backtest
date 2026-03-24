package group.gnometrading.backtest.oms;

import group.gnometrading.oms.position.Position;
import group.gnometrading.oms.position.PositionTracker;
import group.gnometrading.oms.position.PositionView;
import java.util.function.Consumer;

/**
 * PositionView that reads directly from the OMS PositionTracker.
 * Safe for single-threaded use (backtest). Not safe for cross-thread access.
 */
public final class DirectPositionView implements PositionView {

    private final PositionTracker tracker;
    private final int strategyId;

    public DirectPositionView(PositionTracker tracker, int strategyId) {
        this.tracker = tracker;
        this.strategyId = strategyId;
    }

    @Override
    public Position getPosition(int exchangeId, long securityId) {
        return tracker.getStrategyPosition(strategyId, exchangeId, securityId);
    }

    @Override
    public void forEachPosition(Consumer<Position> consumer) {
        tracker.forEachStrategyPosition(strategyId, consumer);
    }

    @Override
    public long getEffectiveQuantity(int exchangeId, long securityId) {
        Position pos = tracker.getStrategyPosition(strategyId, exchangeId, securityId);
        return pos != null ? pos.getEffectiveQuantity() : 0;
    }
}
