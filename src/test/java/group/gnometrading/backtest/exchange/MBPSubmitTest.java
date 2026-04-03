package group.gnometrading.backtest.exchange;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Mbp10Schema;
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

    static final long PRICE_NULL = Long.MIN_VALUE;
    static final long SIZE_NULL = 4294967295L;

    static Mbp10Schema makeSingleLevelUpdate(long bidPx, long bidSz, long askPx, long askSz) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Add);
        schema.encoder.side(Side.None);
        schema.encoder.price(PRICE_NULL);
        schema.encoder.size(SIZE_NULL);
        schema.encoder.bidPrice0(bidPx).bidSize0(bidSz == -1 ? SIZE_NULL : bidSz);
        schema.encoder.askPrice0(askPx).askSize0(askSz == -1 ? SIZE_NULL : askSz);
        schema.encoder
                .bidPrice1(PRICE_NULL)
                .bidSize1(SIZE_NULL)
                .askPrice1(PRICE_NULL)
                .askSize1(SIZE_NULL);
        schema.encoder
                .bidPrice2(PRICE_NULL)
                .bidSize2(SIZE_NULL)
                .askPrice2(PRICE_NULL)
                .askSize2(SIZE_NULL);
        schema.encoder
                .bidPrice3(PRICE_NULL)
                .bidSize3(SIZE_NULL)
                .askPrice3(PRICE_NULL)
                .askSize3(SIZE_NULL);
        schema.encoder
                .bidPrice4(PRICE_NULL)
                .bidSize4(SIZE_NULL)
                .askPrice4(PRICE_NULL)
                .askSize4(SIZE_NULL);
        schema.encoder
                .bidPrice5(PRICE_NULL)
                .bidSize5(SIZE_NULL)
                .askPrice5(PRICE_NULL)
                .askSize5(SIZE_NULL);
        schema.encoder
                .bidPrice6(PRICE_NULL)
                .bidSize6(SIZE_NULL)
                .askPrice6(PRICE_NULL)
                .askSize6(SIZE_NULL);
        schema.encoder
                .bidPrice7(PRICE_NULL)
                .bidSize7(SIZE_NULL)
                .askPrice7(PRICE_NULL)
                .askSize7(SIZE_NULL);
        schema.encoder
                .bidPrice8(PRICE_NULL)
                .bidSize8(SIZE_NULL)
                .askPrice8(PRICE_NULL)
                .askSize8(SIZE_NULL);
        schema.encoder
                .bidPrice9(PRICE_NULL)
                .bidSize9(SIZE_NULL)
                .askPrice9(PRICE_NULL)
                .askSize9(SIZE_NULL);
        return schema;
    }

    static Mbp10Schema makeTwoLevelUpdate(
            long bidPx0, long bidSz0, long askPx0, long askSz0, long bidPx1, long bidSz1, long askPx1, long askSz1) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Add);
        schema.encoder.side(Side.None);
        schema.encoder.price(PRICE_NULL);
        schema.encoder.size(SIZE_NULL);
        schema.encoder.bidPrice0(bidPx0).bidSize0(bidSz0).askPrice0(askPx0).askSize0(askSz0);
        schema.encoder.bidPrice1(bidPx1).bidSize1(bidSz1).askPrice1(askPx1).askSize1(askSz1);
        schema.encoder
                .bidPrice2(PRICE_NULL)
                .bidSize2(SIZE_NULL)
                .askPrice2(PRICE_NULL)
                .askSize2(SIZE_NULL);
        schema.encoder
                .bidPrice3(PRICE_NULL)
                .bidSize3(SIZE_NULL)
                .askPrice3(PRICE_NULL)
                .askSize3(SIZE_NULL);
        schema.encoder
                .bidPrice4(PRICE_NULL)
                .bidSize4(SIZE_NULL)
                .askPrice4(PRICE_NULL)
                .askSize4(SIZE_NULL);
        schema.encoder
                .bidPrice5(PRICE_NULL)
                .bidSize5(SIZE_NULL)
                .askPrice5(PRICE_NULL)
                .askSize5(SIZE_NULL);
        schema.encoder
                .bidPrice6(PRICE_NULL)
                .bidSize6(SIZE_NULL)
                .askPrice6(PRICE_NULL)
                .askSize6(SIZE_NULL);
        schema.encoder
                .bidPrice7(PRICE_NULL)
                .bidSize7(SIZE_NULL)
                .askPrice7(PRICE_NULL)
                .askSize7(SIZE_NULL);
        schema.encoder
                .bidPrice8(PRICE_NULL)
                .bidSize8(SIZE_NULL)
                .askPrice8(PRICE_NULL)
                .askSize8(SIZE_NULL);
        schema.encoder
                .bidPrice9(PRICE_NULL)
                .bidSize9(SIZE_NULL)
                .askPrice9(PRICE_NULL)
                .askSize9(SIZE_NULL);
        return schema;
    }

    static Mbp10Schema makeTrade(Side side, long price, long size) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Trade);
        schema.encoder.side(side);
        schema.encoder.price(price);
        schema.encoder.size(size);
        schema.encoder
                .bidPrice0(PRICE_NULL)
                .bidSize0(SIZE_NULL)
                .askPrice0(PRICE_NULL)
                .askSize0(SIZE_NULL);
        schema.encoder
                .bidPrice1(PRICE_NULL)
                .bidSize1(SIZE_NULL)
                .askPrice1(PRICE_NULL)
                .askSize1(SIZE_NULL);
        schema.encoder
                .bidPrice2(PRICE_NULL)
                .bidSize2(SIZE_NULL)
                .askPrice2(PRICE_NULL)
                .askSize2(SIZE_NULL);
        schema.encoder
                .bidPrice3(PRICE_NULL)
                .bidSize3(SIZE_NULL)
                .askPrice3(PRICE_NULL)
                .askSize3(SIZE_NULL);
        schema.encoder
                .bidPrice4(PRICE_NULL)
                .bidSize4(SIZE_NULL)
                .askPrice4(PRICE_NULL)
                .askSize4(SIZE_NULL);
        schema.encoder
                .bidPrice5(PRICE_NULL)
                .bidSize5(SIZE_NULL)
                .askPrice5(PRICE_NULL)
                .askSize5(SIZE_NULL);
        schema.encoder
                .bidPrice6(PRICE_NULL)
                .bidSize6(SIZE_NULL)
                .askPrice6(PRICE_NULL)
                .askSize6(SIZE_NULL);
        schema.encoder
                .bidPrice7(PRICE_NULL)
                .bidSize7(SIZE_NULL)
                .askPrice7(PRICE_NULL)
                .askSize7(SIZE_NULL);
        schema.encoder
                .bidPrice8(PRICE_NULL)
                .bidSize8(SIZE_NULL)
                .askPrice8(PRICE_NULL)
                .askSize8(SIZE_NULL);
        schema.encoder
                .bidPrice9(PRICE_NULL)
                .bidSize9(SIZE_NULL)
                .askPrice9(PRICE_NULL)
                .askSize9(SIZE_NULL);
        return schema;
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

    // --- Immediate limit crosses ---

    @Test
    void testLimitOrderImmediateFullCross() {
        // Seed: bid 100/50, ask 101/20
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 20));

        // Limit buy at 101, size 20 — fully crosses the resting ask
        List<BacktestExecutionReport> reports = exchange.submitOrder(makeLimitOrder(101, 20, Side.Bid, "BID_CROSS"));

        assertEquals(1, reports.size());
        BacktestExecutionReport r = reports.get(0);
        assertEquals(ExecType.FILL, r.execType);
        assertEquals(OrderStatus.FILLED, r.orderStatus);
        assertEquals(20, r.filledQty);
        assertEquals(0, r.leavesQty);
        assertEquals(20, r.cumulativeQty);
        assertEquals(101, r.fillPrice);
        assertEquals(101 * 20 * 0.05, r.fee, 0.01); // taker fee — aggressive limit crosses the spread
    }

    @Test
    void testLimitOrderPartialCrossGtcRemainder() {
        // Seed: ask 101/10 — only 10 available
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        // Limit buy at 101, size 25, GTC — partial fill, remainder goes on book
        List<BacktestExecutionReport> reports = exchange.submitOrder(makeLimitOrder(101, 25, Side.Bid, "BID_GTC"));

        assertEquals(2, reports.size());
        BacktestExecutionReport newReport = reports.get(0);
        BacktestExecutionReport partialFill = reports.get(1);

        assertEquals(ExecType.NEW, newReport.execType);
        assertEquals(OrderStatus.NEW, newReport.orderStatus);
        assertEquals(25, newReport.leavesQty);

        assertEquals(ExecType.PARTIAL_FILL, partialFill.execType);
        assertEquals(OrderStatus.PARTIALLY_FILLED, partialFill.orderStatus);
        assertEquals(10, partialFill.filledQty);
        assertEquals(15, partialFill.leavesQty);
        assertEquals(101, partialFill.fillPrice);
        assertEquals(101 * 10 * 0.05, partialFill.fee, 0.01); // taker fee
    }

    @Test
    void testLimitOrderPartialCrossIocCanceled() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        BacktestOrder ioc = makeOrder(101, 25, Side.Bid, "BID_IOC", OrderType.LIMIT, TimeInForce.IMMEDIATE_OR_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(ioc);

        assertEquals(2, reports.size());
        assertEquals(ExecType.PARTIAL_FILL, reports.get(0).execType);
        assertEquals(10, reports.get(0).filledQty);
        assertEquals(15, reports.get(0).leavesQty);

        assertEquals(ExecType.CANCEL, reports.get(1).execType);
        assertEquals(OrderStatus.PARTIALLY_FILLED, reports.get(1).orderStatus);
        assertEquals(10, reports.get(1).cumulativeQty);
        assertEquals(0, reports.get(1).leavesQty);
    }

    @Test
    void testLimitOrderPartialCrossFokRejected() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        BacktestOrder fok = makeOrder(101, 25, Side.Bid, "BID_FOK", OrderType.LIMIT, TimeInForce.FILL_OR_KILL);
        List<BacktestExecutionReport> reports = exchange.submitOrder(fok);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
        assertEquals(OrderStatus.REJECTED, reports.get(0).orderStatus);
    }

    // --- Market order edge cases ---

    @Test
    void testMarketOrderMultipleLevelsVwap() {
        // ask 101/20, ask 102/15
        exchange.onMarketData(makeTwoLevelUpdate(100, 50, 101, 20, 99, 30, 102, 15));

        // Market buy 30: fills 20 at 101 and 10 at 102
        // VWAP = (101*20 + 102*10) / 30 = (2020 + 1020) / 30 = 101.33 → 101 (long truncation)
        BacktestOrder order = makeOrder(0, 30, Side.Bid, "MKT_BUY", OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        BacktestExecutionReport r = reports.get(0);
        assertEquals(ExecType.FILL, r.execType);
        assertEquals(30, r.filledQty);
        assertEquals(0, r.leavesQty);
        assertEquals(101, r.fillPrice);
        assertEquals((101 * 20 + 102 * 10) * 0.05, r.fee, 0.01);
    }

    @Test
    void testMarketOrderPartialFill() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        BacktestOrder order =
                makeOrder(0, 25, Side.Bid, "MKT_PARTIAL", OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        BacktestExecutionReport r = reports.get(0);
        assertEquals(ExecType.PARTIAL_FILL, r.execType);
        assertEquals(OrderStatus.PARTIALLY_FILLED, r.orderStatus);
        assertEquals(10, r.filledQty);
        assertEquals(15, r.leavesQty);
        assertEquals(101, r.fillPrice);
    }

    @Test
    void testMarketSellOrder() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 30, 101, 40));

        BacktestOrder order = makeOrder(0, 20, Side.Ask, "MKT_SELL", OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        BacktestExecutionReport r = reports.get(0);
        assertEquals(ExecType.FILL, r.execType);
        assertEquals(20, r.filledQty);
        assertEquals(100, r.fillPrice);
        assertEquals(100 * 20 * 0.05, r.fee, 0.01);
    }

    // --- Amend at exchange level ---

    @Test
    void testAmendOrderPriceChange() {
        exchange.submitOrder(makeLimitOrder(100, 10, Side.Bid, "BID_1"));
        BacktestAmendOrder amend = new BacktestAmendOrder(1, 1, "BID_1", 99, 10);
        List<BacktestExecutionReport> reports = exchange.amendOrder(amend);

        assertEquals(1, reports.size());
        BacktestExecutionReport r = reports.get(0);
        assertEquals(ExecType.NEW, r.execType);
        assertEquals(OrderStatus.NEW, r.orderStatus);
        assertEquals(10, r.leavesQty);
        assertEquals(1, r.exchangeId);
        assertEquals(1, r.securityId);
    }

    @Test
    void testAmendOrderSizeChange() {
        exchange.submitOrder(makeLimitOrder(100, 10, Side.Bid, "BID_1"));
        BacktestAmendOrder amend = new BacktestAmendOrder(1, 1, "BID_1", 100, 15);
        List<BacktestExecutionReport> reports = exchange.amendOrder(amend);

        assertEquals(1, reports.size());
        assertEquals(ExecType.NEW, reports.get(0).execType);
        assertEquals(15, reports.get(0).leavesQty);
    }

    @Test
    void testAmendOrderNonExistentRejected() {
        BacktestAmendOrder amend = new BacktestAmendOrder(1, 1, "NONEXISTENT", 100, 10);
        List<BacktestExecutionReport> reports = exchange.amendOrder(amend);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
        assertEquals(OrderStatus.REJECTED, reports.get(0).orderStatus);
    }

    // --- Execution report field verification ---

    // --- Input validation ---

    @Test
    void testZeroSizeOrderRejected() {
        BacktestOrder order = makeOrder(100, 0, Side.Bid, "ZERO_SIZE", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testNegativeSizeOrderRejected() {
        BacktestOrder order = makeOrder(100, -5, Side.Bid, "NEG_SIZE", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testSideNoneOrderRejected() {
        BacktestOrder order =
                makeOrder(100, 10, Side.None, "NONE_SIDE", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testZeroPriceLimitOrderRejected() {
        BacktestOrder order = makeOrder(0, 10, Side.Bid, "ZERO_PRICE", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    @Test
    void testZeroPriceMarketOrderAllowed() {
        // Market orders have no price, so price=0 should not be rejected for MARKET type
        // (will reject due to no liquidity, not validation)
        BacktestOrder order =
                makeOrder(0, 10, Side.Bid, "MKT_ZERO_PX", OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        // Reject due to no liquidity, not due to validation
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    // --- Self-trade prevention ---

    @Test
    void testSelfTradeMarketOrderRejected() {
        // Seed book with market depth at 101
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 40));
        // Place local ask at 101 — same level as market depth
        exchange.submitOrder(makeLimitOrder(101, 10, Side.Ask, "ASK_REST"));

        // Market buy: encounters our local ask at 101 (where market depth exists) → self-trade → reject
        BacktestOrder mkt = makeOrder(0, 10, Side.Bid, "MKT_BUY", OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<BacktestExecutionReport> reports = exchange.submitOrder(mkt);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).execType);
    }

    // --- Taker fee on aggressive limit cross ---

    @Test
    void testAggressiveLimitOrderChargesTakerFee() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 20));

        // Limit buy at 101 crosses the resting ask — should charge taker fee (5%), not maker (3%)
        List<BacktestExecutionReport> reports = exchange.submitOrder(makeLimitOrder(101, 20, Side.Bid, "BID_CROSS"));

        assertEquals(1, reports.size());
        assertEquals(ExecType.FILL, reports.get(0).execType);
        assertEquals(101 * 20 * 0.05, reports.get(0).fee, 0.01);
    }

    @Test
    void testExecutionReportFieldsOnMakerFill() {
        // Seed ask at 102 with depth 5
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 102, 5));

        // Place local ask at 102, size 10 (phantom=5)
        BacktestOrder askOrder = makeOrder(102, 10, Side.Ask, "ASK_1", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        exchange.submitOrder(askOrder);

        // Trade 15 at 102: phantom=5, remainingVol=10, fills all 10
        List<BacktestExecutionReport> reports = exchange.onMarketData(makeTrade(Side.Bid, 102, 15));

        assertEquals(1, reports.size());
        BacktestExecutionReport r = reports.get(0);
        assertEquals("ASK_1", r.clientOid);
        assertEquals(ExecType.FILL, r.execType);
        assertEquals(OrderStatus.FILLED, r.orderStatus);
        assertEquals(10, r.filledQty);
        assertEquals(102, r.fillPrice);
        assertEquals(10, r.cumulativeQty);
        assertEquals(0, r.leavesQty);
        assertEquals(102 * 10 * 0.03, r.fee, 0.01);
        assertEquals(1, r.exchangeId);
        assertEquals(1, r.securityId);
        assertEquals(Side.Ask, r.side);
    }
}
