package group.gnometrading.backtest.queues;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.book.LocalOrder;
import group.gnometrading.backtest.book.LocalOrderFill;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class QueueModelTest {

    static class DummyQueueModel implements QueueModel {
        @Override
        public void onModify(long prev, long next, ArrayDeque<LocalOrder> queue) {}
    }

    static final AtomicLong clientOidCounter = new AtomicLong(0);

    record LocalOrderSpec(long remaining, long phantomVolume, long cumulativeTradedQuantity) {}

    record ExpectedFill(long clientOid, long fillSize) {}

    record TestCase(
            List<LocalOrderSpec> localOrders,
            long tradeSize,
            List<ExpectedFill> expectedFills,
            List<LocalOrderSpec> expectedState,
            List<Long> expectedDequeClientOids) {}

    static Stream<TestCase> testCases() {
        return Stream.of(
                // No orders
                new TestCase(List.of(), 5, List.of(), List.of(), List.of()),

                // Trade smaller than phantom
                new TestCase(
                        List.of(new LocalOrderSpec(10, 7, 0)),
                        5,
                        List.of(),
                        List.of(new LocalOrderSpec(10, 2, 5)),
                        List.of(1L)),

                // Trade exactly phantom
                new TestCase(
                        List.of(new LocalOrderSpec(10, 5, 0)),
                        5,
                        List.of(),
                        List.of(new LocalOrderSpec(10, 0, 5)),
                        List.of(1L)),

                // Trade consumes phantom and partial fill
                new TestCase(
                        List.of(new LocalOrderSpec(10, 3, 0)),
                        5,
                        List.of(new ExpectedFill(1L, 2)),
                        List.of(new LocalOrderSpec(8, -2, 5)),
                        List.of(1L)),

                // Trade consumes phantom and fills entire order (should be removed)
                new TestCase(
                        List.of(new LocalOrderSpec(2, 1, 0)),
                        5,
                        List.of(new ExpectedFill(1L, 2)),
                        List.of(new LocalOrderSpec(0, -4, 5)),
                        List.of()),

                // Trade fills multiple orders, last order partially filled
                new TestCase(
                        List.of(new LocalOrderSpec(2, 1, 0), new LocalOrderSpec(3, 1, 0), new LocalOrderSpec(4, 1, 0)),
                        7,
                        List.of(new ExpectedFill(1L, 2), new ExpectedFill(2L, 3), new ExpectedFill(3L, 1)),
                        List.of(
                                new LocalOrderSpec(0, -6, 7),
                                new LocalOrderSpec(0, -6, 7),
                                new LocalOrderSpec(3, -6, 7)),
                        List.of(3L)),

                // Trade larger than all orders (all removed)
                new TestCase(
                        List.of(new LocalOrderSpec(2, 1, 0), new LocalOrderSpec(3, 0, 0)),
                        10,
                        List.of(new ExpectedFill(1L, 2), new ExpectedFill(2L, 3)),
                        List.of(new LocalOrderSpec(0, -9, 10), new LocalOrderSpec(0, -10, 10)),
                        List.of()),

                // Trade with zero phantom, partial fill
                new TestCase(
                        List.of(new LocalOrderSpec(5, 0, 0)),
                        3,
                        List.of(new ExpectedFill(1L, 3)),
                        List.of(new LocalOrderSpec(2, -3, 3)),
                        List.of(1L)),

                // Trade with zero phantom, full fill
                new TestCase(
                        List.of(new LocalOrderSpec(3, 0, 0)),
                        3,
                        List.of(new ExpectedFill(1L, 3)),
                        List.of(new LocalOrderSpec(0, -3, 3)),
                        List.of()),

                // Trade with negative phantom, partial fill
                new TestCase(
                        List.of(new LocalOrderSpec(5, -2, 0)),
                        3,
                        List.of(new ExpectedFill(1L, 3)),
                        List.of(new LocalOrderSpec(2, -5, 3)),
                        List.of(1L)),

                // Trade with negative phantom, full fill
                new TestCase(
                        List.of(new LocalOrderSpec(3, -2, 0)),
                        3,
                        List.of(new ExpectedFill(1L, 3)),
                        List.of(new LocalOrderSpec(0, -5, 3)),
                        List.of()),

                // Three orders, all removed
                new TestCase(
                        List.of(new LocalOrderSpec(2, 1, 0), new LocalOrderSpec(3, 0, 0), new LocalOrderSpec(4, 0, 0)),
                        20,
                        List.of(new ExpectedFill(1L, 2), new ExpectedFill(2L, 3), new ExpectedFill(3L, 4)),
                        List.of(
                                new LocalOrderSpec(0, -19, 20),
                                new LocalOrderSpec(0, -20, 20),
                                new LocalOrderSpec(0, -20, 20)),
                        List.of()),

                // Three orders, only first filled (others still have phantom)
                new TestCase(
                        List.of(new LocalOrderSpec(2, 1, 0), new LocalOrderSpec(3, 5, 0), new LocalOrderSpec(4, 5, 0)),
                        3,
                        List.of(new ExpectedFill(1L, 2)),
                        List.of(new LocalOrderSpec(0, -2, 3), new LocalOrderSpec(3, 2, 3), new LocalOrderSpec(4, 2, 3)),
                        List.of(2L, 3L)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testOnTrade(TestCase tc) {
        DummyQueueModel model = new DummyQueueModel();
        ArrayDeque<LocalOrder> deque = new ArrayDeque<>();
        long[] clientOids = new long[tc.localOrders().size()];
        List<LocalOrder> orders = new java.util.ArrayList<>();
        for (int i = 0; i < tc.localOrders().size(); i++) {
            LocalOrderSpec spec = tc.localOrders().get(i);
            long oid = i + 1L;
            clientOids[i] = oid;
            Order order = new Order();
            order.encoder
                    .exchangeId((short) 1)
                    .securityId(1)
                    .price(10000L)
                    .size(10L)
                    .side(Side.Bid)
                    .orderType(OrderType.LIMIT)
                    .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
            order.encodeClientOid(oid, 0);
            LocalOrder lo = new LocalOrder(order, spec.remaining(), spec.phantomVolume());
            lo.cumulativeTradedQuantity = spec.cumulativeTradedQuantity();
            orders.add(lo);
        }

        orders.forEach(deque::addLast);

        List<LocalOrderFill> fills = model.onTrade(tc.tradeSize(), deque);

        // Verify fills
        assertEquals(tc.expectedFills().size(), fills.size(), "Fill count mismatch");
        for (int i = 0; i < tc.expectedFills().size(); i++) {
            ExpectedFill expected = tc.expectedFills().get(i);
            LocalOrderFill actual = fills.get(i);
            assertEquals(
                    expected.clientOid(), actual.localOrder().order.getClientOidCounter(), "Fill clientOid[" + i + "]");
            assertEquals(expected.fillSize(), actual.fillSize(), "Fill size[" + i + "]");
        }

        // Verify state of all orders
        for (int i = 0; i < tc.expectedState().size(); i++) {
            LocalOrderSpec expected = tc.expectedState().get(i);
            LocalOrder actual = orders.get(i);
            assertEquals(expected.remaining(), actual.remaining, "remaining for order " + i);
            assertEquals(expected.phantomVolume(), actual.phantomVolume, "phantomVolume for order " + i);
            assertEquals(
                    expected.cumulativeTradedQuantity(),
                    actual.cumulativeTradedQuantity,
                    "cumulativeTradedQuantity for order " + i);
        }

        // Verify deque contents
        List<Long> dequeOids =
                deque.stream().map(lo -> lo.order.getClientOidCounter()).toList();
        assertEquals(tc.expectedDequeClientOids(), dequeOids, "Deque client OIDs");
    }
}
