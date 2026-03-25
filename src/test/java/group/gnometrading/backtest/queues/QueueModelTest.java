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
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class QueueModelTest {

    static class DummyQueueModel implements QueueModel {
        @Override
        public void onModify(long prev, long next, ArrayDeque<LocalOrder> queue) {}
    }

    record LocalOrderSpec(String clientOid, long remaining, long phantomVolume, long cumulativeTradedQuantity) {}

    record ExpectedFill(String clientOid, long fillSize) {}

    record TestCase(
            List<LocalOrderSpec> localOrders,
            long tradeSize,
            List<ExpectedFill> expectedFills,
            List<LocalOrderSpec> expectedState,
            List<String> expectedDequeClientOids) {}

    static Stream<TestCase> testCases() {
        return Stream.of(
                // No orders
                new TestCase(List.of(), 5, List.of(), List.of(), List.of()),

                // Trade smaller than phantom
                new TestCase(
                        List.of(new LocalOrderSpec("A", 10, 7, 0)),
                        5,
                        List.of(),
                        List.of(new LocalOrderSpec("A", 10, 2, 5)),
                        List.of("A")),

                // Trade exactly phantom
                new TestCase(
                        List.of(new LocalOrderSpec("A", 10, 5, 0)),
                        5,
                        List.of(),
                        List.of(new LocalOrderSpec("A", 10, 0, 5)),
                        List.of("A")),

                // Trade consumes phantom and partial fill
                new TestCase(
                        List.of(new LocalOrderSpec("A", 10, 3, 0)),
                        5,
                        List.of(new ExpectedFill("A", 2)),
                        List.of(new LocalOrderSpec("A", 8, -2, 5)),
                        List.of("A")),

                // Trade consumes phantom and fills entire order (should be removed)
                new TestCase(
                        List.of(new LocalOrderSpec("A", 2, 1, 0)),
                        5,
                        List.of(new ExpectedFill("A", 2)),
                        List.of(new LocalOrderSpec("A", 0, -4, 5)),
                        List.of()),

                // Trade fills multiple orders, last order partially filled
                new TestCase(
                        List.of(
                                new LocalOrderSpec("A", 2, 1, 0),
                                new LocalOrderSpec("B", 3, 1, 0),
                                new LocalOrderSpec("C", 4, 1, 0)),
                        7,
                        List.of(new ExpectedFill("A", 2), new ExpectedFill("B", 3), new ExpectedFill("C", 1)),
                        List.of(
                                new LocalOrderSpec("A", 0, -6, 7),
                                new LocalOrderSpec("B", 0, -6, 7),
                                new LocalOrderSpec("C", 3, -6, 7)),
                        List.of("C")),

                // Trade larger than all orders (all removed)
                new TestCase(
                        List.of(new LocalOrderSpec("A", 2, 1, 0), new LocalOrderSpec("B", 3, 0, 0)),
                        10,
                        List.of(new ExpectedFill("A", 2), new ExpectedFill("B", 3)),
                        List.of(new LocalOrderSpec("A", 0, -9, 10), new LocalOrderSpec("B", 0, -10, 10)),
                        List.of()),

                // Trade with zero phantom, partial fill
                new TestCase(
                        List.of(new LocalOrderSpec("A", 5, 0, 0)),
                        3,
                        List.of(new ExpectedFill("A", 3)),
                        List.of(new LocalOrderSpec("A", 2, -3, 3)),
                        List.of("A")),

                // Trade with zero phantom, full fill
                new TestCase(
                        List.of(new LocalOrderSpec("A", 3, 0, 0)),
                        3,
                        List.of(new ExpectedFill("A", 3)),
                        List.of(new LocalOrderSpec("A", 0, -3, 3)),
                        List.of()),

                // Trade with negative phantom, partial fill
                new TestCase(
                        List.of(new LocalOrderSpec("A", 5, -2, 0)),
                        3,
                        List.of(new ExpectedFill("A", 3)),
                        List.of(new LocalOrderSpec("A", 2, -5, 3)),
                        List.of("A")),

                // Trade with negative phantom, full fill
                new TestCase(
                        List.of(new LocalOrderSpec("A", 3, -2, 0)),
                        3,
                        List.of(new ExpectedFill("A", 3)),
                        List.of(new LocalOrderSpec("A", 0, -5, 3)),
                        List.of()),

                // Three orders, all removed
                new TestCase(
                        List.of(
                                new LocalOrderSpec("A", 2, 1, 0),
                                new LocalOrderSpec("B", 3, 0, 0),
                                new LocalOrderSpec("C", 4, 0, 0)),
                        20,
                        List.of(new ExpectedFill("A", 2), new ExpectedFill("B", 3), new ExpectedFill("C", 4)),
                        List.of(
                                new LocalOrderSpec("A", 0, -19, 20),
                                new LocalOrderSpec("B", 0, -20, 20),
                                new LocalOrderSpec("C", 0, -20, 20)),
                        List.of()),

                // Three orders, only first filled (others still have phantom)
                new TestCase(
                        List.of(
                                new LocalOrderSpec("A", 2, 1, 0),
                                new LocalOrderSpec("B", 3, 5, 0),
                                new LocalOrderSpec("C", 4, 5, 0)),
                        3,
                        List.of(new ExpectedFill("A", 2)),
                        List.of(
                                new LocalOrderSpec("A", 0, -2, 3),
                                new LocalOrderSpec("B", 3, 2, 3),
                                new LocalOrderSpec("C", 4, 2, 3)),
                        List.of("B", "C")));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testOnTrade(TestCase tc) {
        DummyQueueModel model = new DummyQueueModel();
        ArrayDeque<LocalOrder> deque = new ArrayDeque<>();
        List<LocalOrder> orders = tc.localOrders().stream()
                .map(spec -> {
                    BacktestOrder order = new BacktestOrder(
                            1,
                            1,
                            spec.clientOid(),
                            Side.Bid,
                            10000L,
                            10L,
                            OrderType.LIMIT,
                            TimeInForce.GOOD_TILL_CANCELED);
                    LocalOrder lo = new LocalOrder(order, spec.remaining(), spec.phantomVolume());
                    lo.cumulativeTradedQuantity = spec.cumulativeTradedQuantity();
                    return lo;
                })
                .toList();

        orders.forEach(deque::addLast);

        List<LocalOrderFill> fills = model.onTrade(tc.tradeSize(), deque);

        // Verify fills
        assertEquals(tc.expectedFills().size(), fills.size(), "Fill count mismatch");
        for (int i = 0; i < tc.expectedFills().size(); i++) {
            ExpectedFill expected = tc.expectedFills().get(i);
            LocalOrderFill actual = fills.get(i);
            assertEquals(expected.clientOid(), actual.localOrder().order.clientOid(), "Fill clientOid[" + i + "]");
            assertEquals(expected.fillSize(), actual.fillSize(), "Fill size[" + i + "]");
        }

        // Verify state of all orders
        for (int i = 0; i < tc.expectedState().size(); i++) {
            LocalOrderSpec expected = tc.expectedState().get(i);
            LocalOrder actual = orders.get(i);
            assertEquals(expected.remaining(), actual.remaining, "remaining for " + actual.order.clientOid());
            assertEquals(
                    expected.phantomVolume(), actual.phantomVolume, "phantomVolume for " + actual.order.clientOid());
            assertEquals(
                    expected.cumulativeTradedQuantity(),
                    actual.cumulativeTradedQuantity,
                    "cumulativeTradedQuantity for " + actual.order.clientOid());
        }

        // Verify deque contents
        List<String> dequeOids = deque.stream().map(lo -> lo.order.clientOid()).toList();
        assertEquals(tc.expectedDequeClientOids(), dequeOids, "Deque client OIDs");
    }
}
