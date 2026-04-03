package group.gnometrading.backtest.queues;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.book.LocalOrder;
import group.gnometrading.backtest.book.LocalOrderFill;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import java.util.ArrayDeque;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueueModelModifyTest {

    static LocalOrder makeLocalOrder(long phantom, long remaining) {
        BacktestOrder order =
                new BacktestOrder(1, 1, "X", Side.Bid, 100L, 10L, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        return new LocalOrder(order, remaining, phantom);
    }

    static ArrayDeque<LocalOrder> dequeOf(LocalOrder... orders) {
        ArrayDeque<LocalOrder> deque = new ArrayDeque<>();
        for (LocalOrder o : orders) {
            deque.addLast(o);
        }
        return deque;
    }

    // --- OptimisticQueueModel ---

    @Test
    void testOptimisticOnModify_depthDecrease() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 15, dequeOf(order));
        assertEquals(5, order.phantomVolume);
    }

    @Test
    void testOptimisticOnModify_depthDecreaseFlooredAtZero() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        LocalOrder order = makeLocalOrder(3, 5);
        model.onModify(20, 5, dequeOf(order));
        assertEquals(0, order.phantomVolume);
    }

    @Test
    void testOptimisticOnModify_depthIncrease() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 25, dequeOf(order));
        assertEquals(10, order.phantomVolume);
    }

    @Test
    void testOptimisticOnModify_emptyQueue() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        assertDoesNotThrow(() -> model.onModify(20, 10, new ArrayDeque<>()));
    }

    @Test
    void testOptimisticOnModify_multipleOrders() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        LocalOrder a = makeLocalOrder(10, 5);
        LocalOrder b = makeLocalOrder(7, 3);
        model.onModify(20, 14, dequeOf(a, b));
        assertEquals(4, a.phantomVolume);
        assertEquals(1, b.phantomVolume);
    }

    // --- RiskAverseQueueModel ---

    @Test
    void testRiskAverseOnModify_depthDecrease() {
        RiskAverseQueueModel model = new RiskAverseQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 5, dequeOf(order));
        assertEquals(5, order.phantomVolume);
    }

    @Test
    void testRiskAverseOnModify_depthIncrease() {
        RiskAverseQueueModel model = new RiskAverseQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 25, dequeOf(order));
        assertEquals(10, order.phantomVolume);
    }

    @Test
    void testRiskAverseOnModify_depthDropsToZero() {
        RiskAverseQueueModel model = new RiskAverseQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 0, dequeOf(order));
        assertEquals(0, order.phantomVolume);
    }

    @Test
    void testRiskAverseOnModify_multipleOrders() {
        RiskAverseQueueModel model = new RiskAverseQueueModel();
        LocalOrder a = makeLocalOrder(10, 5);
        LocalOrder b = makeLocalOrder(3, 5);
        model.onModify(20, 5, dequeOf(a, b));
        assertEquals(5, a.phantomVolume);
        assertEquals(3, b.phantomVolume);
    }

    @Test
    void testRiskAverseOnModify_emptyQueue() {
        RiskAverseQueueModel model = new RiskAverseQueueModel();
        assertDoesNotThrow(() -> model.onModify(20, 5, new ArrayDeque<>()));
    }

    // --- ProbabilisticQueueModel ---

    @Test
    void testProbabilisticOnModify_halfProbability() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(0.5);
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 10, dequeOf(order));
        // removed=10, expectedAhead=round(0.5*10)=5, phantom=10-5=5
        assertEquals(5, order.phantomVolume);
    }

    @Test
    void testProbabilisticOnModify_zeroProbability() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(0.0);
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 10, dequeOf(order));
        // expectedAhead=round(0.0*10)=0, early return
        assertEquals(10, order.phantomVolume);
    }

    @Test
    void testProbabilisticOnModify_oneProbability() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(1.0);
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 10, dequeOf(order));
        // removed=10, expectedAhead=10, phantom=10-10=0
        assertEquals(0, order.phantomVolume);
    }

    @Test
    void testProbabilisticOnModify_flooredAtZero() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(1.0);
        LocalOrder order = makeLocalOrder(3, 5);
        model.onModify(20, 5, dequeOf(order));
        // removed=15, expectedAhead=15, phantom=max(0, 3-15)=0
        assertEquals(0, order.phantomVolume);
    }

    @Test
    void testProbabilisticOnModify_depthIncrease() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(0.5);
        LocalOrder order = makeLocalOrder(10, 5);
        model.onModify(20, 25, dequeOf(order));
        // removedVolume=-5, <=0, early return
        assertEquals(10, order.phantomVolume);
    }

    @Test
    void testProbabilisticOnModify_emptyQueue() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(0.5);
        assertDoesNotThrow(() -> model.onModify(20, 10, new ArrayDeque<>()));
    }

    @Test
    void testProbabilisticOnModify_multipleOrders() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(0.5);
        LocalOrder a = makeLocalOrder(10, 5);
        LocalOrder b = makeLocalOrder(4, 5);
        model.onModify(20, 10, dequeOf(a, b));
        // removed=10, expectedAhead=5
        assertEquals(5, a.phantomVolume);
        assertEquals(0, b.phantomVolume);
    }

    @Test
    void testProbabilisticConstructor_negativeProbability() {
        assertThrows(IllegalArgumentException.class, () -> new ProbabilisticQueueModel(-0.1));
    }

    @Test
    void testProbabilisticConstructor_aboveOneProbability() {
        assertThrows(IllegalArgumentException.class, () -> new ProbabilisticQueueModel(1.1));
    }

    @Test
    void testProbabilisticConstructor_boundaryValues() {
        assertDoesNotThrow(() -> new ProbabilisticQueueModel(0.0));
        assertDoesNotThrow(() -> new ProbabilisticQueueModel(1.0));
    }

    // --- Interaction: onModify followed by onTrade ---

    @Test
    void testOptimisticOnModify_thenOnTrade_fillsEarlier() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        ArrayDeque<LocalOrder> deque = dequeOf(order);

        // Depth drops from 20 to 12, removing 8: phantom drops from 10 to 2
        model.onModify(20, 12, deque);
        assertEquals(2, order.phantomVolume);

        // Trade 5: phantom=2, consumedPhantom=2, remainingVol=5-2=3, fill min(5,3)=3
        List<LocalOrderFill> fills = model.onTrade(5, deque);

        assertEquals(1, fills.size());
        assertEquals(3, fills.get(0).fillSize());
        assertEquals(2, order.remaining);
    }

    @Test
    void testRiskAverseOnModify_thenOnTrade_fillsEarlier() {
        RiskAverseQueueModel model = new RiskAverseQueueModel();
        LocalOrder order = makeLocalOrder(10, 5);
        ArrayDeque<LocalOrder> deque = dequeOf(order);

        // Depth drops to 3: phantom clamped to min(10, 3) = 3
        model.onModify(20, 3, deque);
        assertEquals(3, order.phantomVolume);

        // Trade 5: phantom=3, consumedPhantom=3, remainingVol=5-3=2, fill min(5,2)=2
        List<LocalOrderFill> fills = model.onTrade(5, deque);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).fillSize());
        assertEquals(3, order.remaining);
    }

    @Test
    void testOptimisticOnModify_multipleModifiesThenTrade() {
        OptimisticQueueModel model = new OptimisticQueueModel();
        LocalOrder order = makeLocalOrder(20, 10);
        ArrayDeque<LocalOrder> deque = dequeOf(order);

        // Two consecutive depth decreases
        model.onModify(30, 25, deque); // removed=5, phantom=20-5=15
        model.onModify(25, 18, deque); // removed=7, phantom=15-7=8

        assertEquals(8, order.phantomVolume);

        // Trade 10: phantom=8, remainingVol=10-8=2, fill min(10,2)=2
        List<LocalOrderFill> fills = model.onTrade(10, deque);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).fillSize());
    }

    @Test
    void testProbabilisticOnModify_thenOnTrade() {
        ProbabilisticQueueModel model = new ProbabilisticQueueModel(0.5);
        LocalOrder order = makeLocalOrder(10, 8);
        ArrayDeque<LocalOrder> deque = dequeOf(order);

        // removed=10, expectedAhead=round(0.5*10)=5, phantom=10-5=5
        model.onModify(20, 10, deque);
        assertEquals(5, order.phantomVolume);

        // Trade 7: phantom=5, remainingVol=7-5=2, fill min(8,2)=2
        List<LocalOrderFill> fills = model.onTrade(7, deque);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).fillSize());
        assertEquals(6, order.remaining);
    }
}
