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

class MBPMarketDataTest {

    static final long PRICE_NULL = Long.MIN_VALUE;
    static final long SIZE_NULL = 4294967295L;

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

    /** Creates an MBP10 market update with up to two bid/ask levels. */
    static Mbp10Schema makeMarketUpdate(
            Action action,
            long bidPx0,
            long bidSz0,
            long askPx0,
            long askSz0,
            long bidPx1,
            long bidSz1,
            long askPx1,
            long askSz1) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(action);
        schema.encoder.side(Side.None);
        schema.encoder.price(PRICE_NULL);
        schema.encoder.size(SIZE_NULL);
        schema.encoder.bidPrice0(bidPx0);
        schema.encoder.bidSize0(bidSz0 == -1 ? SIZE_NULL : bidSz0);
        schema.encoder.askPrice0(askPx0);
        schema.encoder.askSize0(askSz0 == -1 ? SIZE_NULL : askSz0);
        schema.encoder.bidPrice1(bidPx1);
        schema.encoder.bidSize1(bidSz1 == -1 ? SIZE_NULL : bidSz1);
        schema.encoder.askPrice1(askPx1);
        schema.encoder.askSize1(askSz1 == -1 ? SIZE_NULL : askSz1);
        // Fill remaining levels with null
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

    static Mbp10Schema makeSingleLevelUpdate(long bidPx, long bidSz, long askPx, long askSz) {
        return makeMarketUpdate(Action.Add, bidPx, bidSz, askPx, askSz, PRICE_NULL, -1, PRICE_NULL, -1);
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

    @Test
    void testMarketUpdateSeedsBidAsk() {
        // Just verifying that onMarketData doesn't throw when no local orders
        Mbp10Schema update = makeSingleLevelUpdate(100, 50, 102, 40);
        List<BacktestExecutionReport> reports = exchange.onMarketData(update);
        assertTrue(reports.isEmpty());
    }

    @Test
    void testTradeNoLocalOrdersProducesNoReports() {
        Mbp10Schema update = makeSingleLevelUpdate(100, 50, 102, 40);
        exchange.onMarketData(update);

        Mbp10Schema trade = makeTrade(Side.Bid, 102, 10);
        List<BacktestExecutionReport> reports = exchange.onMarketData(trade);
        assertTrue(reports.isEmpty());
    }

    @Test
    void testTradeFilledLocalAskOrder() {
        // Seed the book
        Mbp10Schema update = makeSingleLevelUpdate(100, 50, 102, 40);
        exchange.onMarketData(update);

        // Place a local ask at 102 (phantom = 40 since that's the market depth)
        BacktestOrder askOrder = makeOrder(102, 8, Side.Ask, "ASK_1", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        exchange.submitOrder(askOrder);

        // Trade: buy 50 at 102 (phantom=40, trade=50 → 50-40=10 fills, but order is only 8)
        Mbp10Schema trade = makeTrade(Side.Bid, 102, 50);
        List<BacktestExecutionReport> reports = exchange.onMarketData(trade);

        assertFalse(reports.isEmpty());
        BacktestExecutionReport report = reports.get(0);
        assertEquals("ASK_1", report.clientOid);
        assertEquals(ExecType.FILL, report.execType);
        assertEquals(OrderStatus.FILLED, report.orderStatus);
        assertEquals(8, report.filledQty);
        assertEquals(0, report.leavesQty);
    }

    @Test
    void testTradePartialFillLocalOrder() {
        // Seed the book
        Mbp10Schema update = makeSingleLevelUpdate(100, 50, 102, 5);
        exchange.onMarketData(update);

        // Local ask with phantom = 5 (market depth at 102)
        BacktestOrder askOrder = makeOrder(102, 20, Side.Ask, "ASK_1", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        exchange.submitOrder(askOrder);

        // Trade 8 at 102: phantom=5, trade=8 → fills 3 of our 20 remaining
        Mbp10Schema trade = makeTrade(Side.Bid, 102, 8);
        List<BacktestExecutionReport> reports = exchange.onMarketData(trade);

        assertFalse(reports.isEmpty());
        BacktestExecutionReport report = reports.get(0);
        assertEquals("ASK_1", report.clientOid);
        assertEquals(ExecType.FILL, report.execType);
        assertEquals(OrderStatus.PARTIALLY_FILLED, report.orderStatus);
        assertEquals(3, report.filledQty);
        assertEquals(17, report.leavesQty);
    }

    @Test
    void testTradeFilledLocalBidOrder() {
        // Seed the book
        Mbp10Schema update = makeSingleLevelUpdate(100, 10, 102, 40);
        exchange.onMarketData(update);

        // Local bid at 100 with phantom = 10 (market depth at 100)
        BacktestOrder bidOrder = makeOrder(100, 5, Side.Bid, "BID_1", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        exchange.submitOrder(bidOrder);

        // Sell trade at 100: phantom=10, trade=20 → fills 5 of our bid
        Mbp10Schema trade = makeTrade(Side.Ask, 100, 20);
        List<BacktestExecutionReport> reports = exchange.onMarketData(trade);

        assertFalse(reports.isEmpty());
        assertEquals("BID_1", reports.get(0).clientOid);
        assertEquals(OrderStatus.FILLED, reports.get(0).orderStatus);
    }

    @Test
    void testFeeCalculationOnFill() {
        // phantom=5 at ask=102, so trade=15 → remainingVolume=15-5=10 → fills all 10 of ASK_1
        Mbp10Schema update = makeSingleLevelUpdate(100, 0, 102, 5);
        exchange.onMarketData(update);

        BacktestOrder askOrder = makeOrder(102, 10, Side.Ask, "ASK_1", OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
        exchange.submitOrder(askOrder);

        // Trade fills ASK_1: price=102, size=10, total=1020. Maker fee = 3% = 30.6
        Mbp10Schema trade = makeTrade(Side.Bid, 102, 15);
        List<BacktestExecutionReport> reports = exchange.onMarketData(trade);

        assertFalse(reports.isEmpty());
        assertEquals(102 * 10 * 0.03, reports.get(0).fee, 0.01);
    }
}
