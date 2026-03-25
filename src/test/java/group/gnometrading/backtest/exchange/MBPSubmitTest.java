package group.gnometrading.backtest.exchange;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.book.BidAskLevel;
import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import java.util.ArrayDeque;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MBPSubmitTest {

    static class DummyQueueModel implements QueueModel {
        @Override
        public void onModify(long prev, long next, ArrayDeque<group.gnometrading.backtest.book.LocalOrder> queue) {}
    }

    static class DummyFeeModel implements FeeModel {
        @Override
        public double calculateFee(double totalPrice, boolean isMaker) {
            return totalPrice * (isMaker ? 0.03 : 0.05);
        }
    }

    static class ZeroLatency implements LatencyModel {
        @Override
        public long simulate() {
            return 0;
        }
    }

    MbpSimulatedExchange exchange;

    @BeforeEach
    void setUp() {
        exchange = new MbpSimulatedExchange(
                new DummyFeeModel(), new ZeroLatency(), new ZeroLatency(), new DummyQueueModel());
    }

    void setupBook(List<BidAskLevel> levels) {
        // We can't directly call onMarketUpdate on the exchange from here since it
        // requires a Schema object. Use the test-visible MBP10 setup helper via a
        // wrapper that calls the internal book methods.
        // Instead, test submitOrder through exchange directly with a pre-seeded book.
        // Since we can't reach the internal book here, we test through a subclass.
        // For now, use the ExchangeTestHelper approach.
    }

    static BacktestOrder makeOrder(
            long price, long size, Side side, String clientOid, OrderType orderType, TimeInForce tif) {
        return new BacktestOrder(1, 1, clientOid, side, price, size, orderType, tif);
    }

    static BacktestOrder makeLimitOrder(long price, long size, Side side, String clientOid) {
        return makeOrder(price, size, side, clientOid, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
    }

    static BacktestOrder makeMarketOrder(long size, Side side, String clientOid) {
        return makeOrder(0, size, side, clientOid, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
    }

    @Test
    void testMarketOrderNoLiquidityRejected() {
        BacktestOrder order = makeMarketOrder(10, Side.Bid, "MARKET_NO_LIQ");
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
        assertEquals(OrderStatus.REJECTED, reports.get(0).orderStatus);
    }

    @Test
    void testGtcLimitOrderAddedToBook() {
        BacktestOrder order = makeLimitOrder(100, 10, Side.Bid, "GTC_BID");
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.NEW, reports.get(0).execType);
        assertEquals(OrderStatus.NEW, reports.get(0).orderStatus);
        assertEquals(10, reports.get(0).leavesQty);
    }

    @Test
    void testIocLimitOrderNoFillRejected() {
        BacktestOrder order =
                makeOrder(100, 10, Side.Bid, "IOC_NO_FILL", OrderType.LIMIT, TimeInForce.IMMEDIATE_OR_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testFokLimitOrderNoFillRejected() {
        BacktestOrder order = makeOrder(100, 10, Side.Bid, "FOK_NO_FILL", OrderType.LIMIT, TimeInForce.FILL_OR_KILL);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testCancelExistingOrder() {
        BacktestOrder order = makeLimitOrder(100, 10, Side.Bid, "GTC_BID");
        exchange.submitOrder(order);

        BacktestCancelOrder cancel = new BacktestCancelOrder(1, 1, "GTC_BID");
        List<BacktestExecutionReport> reports = exchange.cancelOrder(cancel);

        assertEquals(1, reports.size());
        assertEquals(ExecType.CANCEL, reports.get(0).execType);
        assertEquals(OrderStatus.CANCELED, reports.get(0).orderStatus);
    }

    @Test
    void testCancelNonExistentOrder() {
        BacktestCancelOrder cancel = new BacktestCancelOrder(1, 1, "NONEXISTENT");
        List<BacktestExecutionReport> reports = exchange.cancelOrder(cancel);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testAutoGeneratedClientOid() {
        BacktestOrder orderWithNull =
                new BacktestOrder(1, 1, null, Side.Bid, 100, 10, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(orderWithNull);

        assertEquals(1, reports.size());
        assertNotNull(reports.get(0).clientOid);
        assertFalse(reports.get(0).clientOid.isEmpty());
    }

    @Test
    void testMultipleGtcOrdersAddedToBook() {
        List<BacktestExecutionReport> r1 = exchange.submitOrder(makeLimitOrder(100, 10, Side.Bid, "BID_1"));
        List<BacktestExecutionReport> r2 = exchange.submitOrder(makeLimitOrder(99, 5, Side.Bid, "BID_2"));
        List<BacktestExecutionReport> r3 = exchange.submitOrder(makeLimitOrder(101, 8, Side.Ask, "ASK_1"));

        assertEquals(ExecType.NEW, r1.get(0).execType);
        assertEquals(ExecType.NEW, r2.get(0).execType);
        assertEquals(ExecType.NEW, r3.get(0).execType);
    }
}
