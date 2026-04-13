package group.gnometrading.backtest.exchange;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
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

    static Order makeOrder(long price, long size, Side side, long clientOid, OrderType orderType, TimeInForce tif) {
        Order order = new Order();
        order.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(price)
                .size(size)
                .side(side)
                .orderType(orderType)
                .timeInForce(tif);
        order.encodeClientOid(clientOid, 0);
        return order;
    }

    static Order makeLimitOrder(long price, long size, Side side, long clientOid) {
        return makeOrder(price, size, side, clientOid, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
    }

    static Order makeMarketOrder(long size, Side side, long clientOid) {
        return makeOrder(0, size, side, clientOid, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
    }

    static CancelOrder makeCancel(int exchangeId, int securityId, long clientOid) {
        CancelOrder cancel = new CancelOrder();
        cancel.encoder.exchangeId((short) exchangeId).securityId(securityId);
        cancel.encodeClientOid(clientOid, 0);
        return cancel;
    }

    static ModifyOrder makeModify(int exchangeId, int securityId, long clientOid, long newPrice, long newSize) {
        ModifyOrder modify = new ModifyOrder();
        modify.encoder
                .exchangeId((short) exchangeId)
                .securityId(securityId)
                .price(newPrice)
                .size(newSize);
        modify.encodeClientOid(clientOid, 0);
        return modify;
    }

    static double decodeFee(OrderExecutionReport report) {
        return report.decoder.fee() / (double) Statics.PRICE_SCALING_FACTOR;
    }

    @Test
    void testMarketOrderNoLiquidityRejected() {
        Order order = makeMarketOrder(10, Side.Bid, 1L);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
        assertEquals(OrderStatus.REJECTED, reports.get(0).decoder.orderStatus());
    }

    @Test
    void testGtcLimitOrderAddedToBook() {
        Order order = makeLimitOrder(100, 10, Side.Bid, 1L);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.NEW, reports.get(0).decoder.execType());
        assertEquals(OrderStatus.NEW, reports.get(0).decoder.orderStatus());
        assertEquals(10, reports.get(0).decoder.leavesQty());
    }

    @Test
    void testIocLimitOrderNoFillRejected() {
        Order order = makeOrder(100, 10, Side.Bid, 1L, OrderType.LIMIT, TimeInForce.IMMEDIATE_OR_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    @Test
    void testFokLimitOrderNoFillRejected() {
        Order order = makeOrder(100, 10, Side.Bid, 1L, OrderType.LIMIT, TimeInForce.FILL_OR_KILL);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    @Test
    void testCancelExistingOrder() {
        Order order = makeLimitOrder(100, 10, Side.Bid, 1L);
        exchange.submitOrder(order);

        CancelOrder cancel = makeCancel(1, 1, 1L);
        List<OrderExecutionReport> reports = exchange.cancelOrder(cancel);

        assertEquals(1, reports.size());
        assertEquals(ExecType.CANCEL, reports.get(0).decoder.execType());
        assertEquals(OrderStatus.CANCELED, reports.get(0).decoder.orderStatus());
    }

    @Test
    void testCancelNonExistentOrder() {
        CancelOrder cancel = makeCancel(1, 1, 99L);
        List<OrderExecutionReport> reports = exchange.cancelOrder(cancel);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    @Test
    void testAutoGeneratedClientOid() {
        Order order = makeOrder(100, 10, Side.Bid, 0L, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertTrue(reports.get(0).getClientOidCounter() > 0);
    }

    @Test
    void testMultipleGtcOrdersAddedToBook() {
        List<OrderExecutionReport> r1 = exchange.submitOrder(makeLimitOrder(100, 10, Side.Bid, 1L));
        List<OrderExecutionReport> r2 = exchange.submitOrder(makeLimitOrder(99, 5, Side.Bid, 2L));
        List<OrderExecutionReport> r3 = exchange.submitOrder(makeLimitOrder(101, 8, Side.Ask, 3L));

        assertEquals(ExecType.NEW, r1.get(0).decoder.execType());
        assertEquals(ExecType.NEW, r2.get(0).decoder.execType());
        assertEquals(ExecType.NEW, r3.get(0).decoder.execType());
    }

    // --- Immediate limit crosses ---

    @Test
    void testLimitOrderImmediateFullCross() {
        // Seed: bid 100/50, ask 101/20
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 20));

        // Limit buy at 101, size 20 — fully crosses the resting ask
        List<OrderExecutionReport> reports = exchange.submitOrder(makeLimitOrder(101, 20, Side.Bid, 1L));

        assertEquals(1, reports.size());
        OrderExecutionReport r = reports.get(0);
        assertEquals(ExecType.FILL, r.decoder.execType());
        assertEquals(OrderStatus.FILLED, r.decoder.orderStatus());
        assertEquals(20, r.decoder.filledQty());
        assertEquals(0, r.decoder.leavesQty());
        assertEquals(20, r.decoder.cumulativeQty());
        assertEquals(101, r.decoder.fillPrice());
        assertEquals(101 * 20 * 0.05, decodeFee(r), 0.01); // taker fee — aggressive limit crosses the spread
    }

    @Test
    void testLimitOrderPartialCrossGtcRemainder() {
        // Seed: ask 101/10 — only 10 available
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        // Limit buy at 101, size 25, GTC — partial fill, remainder goes on book
        List<OrderExecutionReport> reports = exchange.submitOrder(makeLimitOrder(101, 25, Side.Bid, 1L));

        assertEquals(2, reports.size());
        OrderExecutionReport newReport = reports.get(0);
        OrderExecutionReport partialFill = reports.get(1);

        assertEquals(ExecType.NEW, newReport.decoder.execType());
        assertEquals(OrderStatus.NEW, newReport.decoder.orderStatus());
        assertEquals(25, newReport.decoder.leavesQty());

        assertEquals(ExecType.PARTIAL_FILL, partialFill.decoder.execType());
        assertEquals(OrderStatus.PARTIALLY_FILLED, partialFill.decoder.orderStatus());
        assertEquals(10, partialFill.decoder.filledQty());
        assertEquals(15, partialFill.decoder.leavesQty());
        assertEquals(101, partialFill.decoder.fillPrice());
        assertEquals(101 * 10 * 0.05, decodeFee(partialFill), 0.01); // taker fee
    }

    @Test
    void testLimitOrderPartialCrossIocCanceled() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        Order ioc = makeOrder(101, 25, Side.Bid, 1L, OrderType.LIMIT, TimeInForce.IMMEDIATE_OR_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(ioc);

        assertEquals(2, reports.size());
        assertEquals(ExecType.PARTIAL_FILL, reports.get(0).decoder.execType());
        assertEquals(10, reports.get(0).decoder.filledQty());
        assertEquals(15, reports.get(0).decoder.leavesQty());

        assertEquals(ExecType.CANCEL, reports.get(1).decoder.execType());
        assertEquals(OrderStatus.PARTIALLY_FILLED, reports.get(1).decoder.orderStatus());
        assertEquals(10, reports.get(1).decoder.cumulativeQty());
        assertEquals(0, reports.get(1).decoder.leavesQty());
    }

    @Test
    void testLimitOrderPartialCrossFokRejected() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        Order fok = makeOrder(101, 25, Side.Bid, 1L, OrderType.LIMIT, TimeInForce.FILL_OR_KILL);
        List<OrderExecutionReport> reports = exchange.submitOrder(fok);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
        assertEquals(OrderStatus.REJECTED, reports.get(0).decoder.orderStatus());
    }

    // --- Market order edge cases ---

    @Test
    void testMarketOrderMultipleLevelsVwap() {
        // ask 101/20, ask 102/15
        exchange.onMarketData(makeTwoLevelUpdate(100, 50, 101, 20, 99, 30, 102, 15));

        // Market buy 30: fills 20 at 101 and 10 at 102
        // VWAP = (101*20 + 102*10) / 30 = (2020 + 1020) / 30 = 101.33 → 101 (long truncation)
        Order order = makeOrder(0, 30, Side.Bid, 1L, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        OrderExecutionReport r = reports.get(0);
        assertEquals(ExecType.FILL, r.decoder.execType());
        assertEquals(30, r.decoder.filledQty());
        assertEquals(0, r.decoder.leavesQty());
        assertEquals(101, r.decoder.fillPrice());
        assertEquals((101 * 20 + 102 * 10) * 0.05, decodeFee(r), 0.01);
    }

    @Test
    void testMarketOrderPartialFill() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 10));

        Order order = makeOrder(0, 25, Side.Bid, 1L, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        OrderExecutionReport r = reports.get(0);
        assertEquals(ExecType.PARTIAL_FILL, r.decoder.execType());
        assertEquals(OrderStatus.PARTIALLY_FILLED, r.decoder.orderStatus());
        assertEquals(10, r.decoder.filledQty());
        assertEquals(15, r.decoder.leavesQty());
        assertEquals(101, r.decoder.fillPrice());
    }

    @Test
    void testMarketSellOrder() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 30, 101, 40));

        Order order = makeOrder(0, 20, Side.Ask, 1L, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        OrderExecutionReport r = reports.get(0);
        assertEquals(ExecType.FILL, r.decoder.execType());
        assertEquals(20, r.decoder.filledQty());
        assertEquals(100, r.decoder.fillPrice());
        assertEquals(100 * 20 * 0.05, decodeFee(r), 0.01);
    }

    // --- Modify at exchange level ---

    @Test
    void testModifyOrderPriceChange() {
        exchange.submitOrder(makeLimitOrder(100, 10, Side.Bid, 1L));
        ModifyOrder modify = makeModify(1, 1, 1L, 99, 10);
        List<OrderExecutionReport> reports = exchange.modifyOrder(modify);

        assertEquals(1, reports.size());
        OrderExecutionReport r = reports.get(0);
        assertEquals(ExecType.NEW, r.decoder.execType());
        assertEquals(OrderStatus.NEW, r.decoder.orderStatus());
        assertEquals(10, r.decoder.leavesQty());
        assertEquals(1, r.decoder.exchangeId());
        assertEquals(1, r.decoder.securityId());
    }

    @Test
    void testModifyOrderSizeChange() {
        exchange.submitOrder(makeLimitOrder(100, 10, Side.Bid, 1L));
        ModifyOrder modify = makeModify(1, 1, 1L, 100, 15);
        List<OrderExecutionReport> reports = exchange.modifyOrder(modify);

        assertEquals(1, reports.size());
        assertEquals(ExecType.NEW, reports.get(0).decoder.execType());
        assertEquals(15, reports.get(0).decoder.leavesQty());
    }

    @Test
    void testModifyOrderNonExistentRejected() {
        ModifyOrder modify = makeModify(1, 1, 99L, 100, 10);
        List<OrderExecutionReport> reports = exchange.modifyOrder(modify);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
        assertEquals(OrderStatus.REJECTED, reports.get(0).decoder.orderStatus());
    }

    // --- Input validation ---

    @Test
    void testZeroSizeOrderRejected() {
        Order order = makeOrder(100, 0, Side.Bid, 1L, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    @Test
    void testSideNoneOrderRejected() {
        Order order = makeOrder(100, 10, Side.None, 1L, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    @Test
    void testZeroPriceLimitOrderRejected() {
        Order order = makeOrder(0, 10, Side.Bid, 1L, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    @Test
    void testZeroPriceMarketOrderAllowed() {
        // Market orders have no price, so price=0 should not be rejected for MARKET type
        // (will reject due to no liquidity, not validation)
        Order order = makeOrder(0, 10, Side.Bid, 1L, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(order);

        assertEquals(1, reports.size());
        // Reject due to no liquidity, not due to validation
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    // --- Self-trade prevention ---

    @Test
    void testSelfTradeMarketOrderRejected() {
        // Seed book with market depth at 101
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 40));
        // Place local ask at 101 — same level as market depth
        exchange.submitOrder(makeLimitOrder(101, 10, Side.Ask, 1L));

        // Market buy: encounters our local ask at 101 (where market depth exists) → self-trade → reject
        Order mkt = makeOrder(0, 10, Side.Bid, 2L, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderExecutionReport> reports = exchange.submitOrder(mkt);

        assertEquals(1, reports.size());
        assertEquals(ExecType.REJECT, reports.get(0).decoder.execType());
    }

    // --- Taker fee on aggressive limit cross ---

    @Test
    void testAggressiveLimitOrderChargesTakerFee() {
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 101, 20));

        // Limit buy at 101 crosses the resting ask — should charge taker fee (5%), not maker (3%)
        List<OrderExecutionReport> reports = exchange.submitOrder(makeLimitOrder(101, 20, Side.Bid, 1L));

        assertEquals(1, reports.size());
        assertEquals(ExecType.FILL, reports.get(0).decoder.execType());
        assertEquals(101 * 20 * 0.05, decodeFee(reports.get(0)), 0.01);
    }

    @Test
    void testExecutionReportFieldsOnMakerFill() {
        // Seed ask at 102 with depth 5
        exchange.onMarketData(makeSingleLevelUpdate(100, 50, 102, 5));

        // Place local ask at 102, size 10 (phantom=5)
        Order askOrder = makeOrder(102, 10, Side.Ask, 1L, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        exchange.submitOrder(askOrder);

        // Trade 15 at 102: phantom=5, remainingVol=10, fills all 10
        List<OrderExecutionReport> reports = exchange.onMarketData(makeTrade(Side.Bid, 102, 15));

        assertEquals(1, reports.size());
        OrderExecutionReport r = reports.get(0);
        assertEquals(1L, r.getClientOidCounter());
        assertEquals(ExecType.FILL, r.decoder.execType());
        assertEquals(OrderStatus.FILLED, r.decoder.orderStatus());
        assertEquals(10, r.decoder.filledQty());
        assertEquals(102, r.decoder.fillPrice());
        assertEquals(10, r.decoder.cumulativeQty());
        assertEquals(0, r.decoder.leavesQty());
        assertEquals(102 * 10 * 0.03, decodeFee(r), 0.01);
        assertEquals(1, r.decoder.exchangeId());
        assertEquals(1, r.decoder.securityId());
    }
}
