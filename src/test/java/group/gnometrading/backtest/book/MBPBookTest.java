package group.gnometrading.backtest.book;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import java.util.ArrayDeque;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MBPBookTest {

    static class DummyQueueModel implements QueueModel {
        @Override
        public void onModify(long prev, long next, ArrayDeque<LocalOrder> queue) {}
    }

    static BacktestOrder makeOrder(long price, long size, Side side, String clientOid) {
        return new BacktestOrder(1, 1, clientOid, side, price, size, OrderType.LIMIT, TimeInForce.GOOD_TILL_CANCELED);
    }

    static BidAskLevel makeBidAskLevel(long bidPx, long bidSz, long askPx, long askSz) {
        return new BidAskLevel(bidPx, bidSz, askPx, askSz);
    }

    MbpBook book;

    @BeforeEach
    void setUp() {
        book = new MbpBook(new DummyQueueModel());
    }

    @Test
    void testInitialMarketUpdate() {
        List<BidAskLevel> levels = List.of(
                makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35), makeBidAskLevel(98, 25, 103, 30));
        book.onMarketUpdate(levels);

        assertEquals(100L, book.getBestBid());
        assertEquals(101L, book.getBestAsk());
        assertEquals(50, book.bids().get(100L).size);
        assertEquals(40, book.asks().get(101L).size);
        assertEquals(30, book.bids().get(99L).size);
        assertEquals(35, book.asks().get(102L).size);
    }

    @Test
    void testAddLocalOrderBid() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertEquals(1, book.localBidOrders().size());
        assertTrue(book.localBidOrders().containsKey("BID_1"));
        LocalOrder localOrder = book.localBidOrders().get("BID_1");
        assertEquals(10, localOrder.remaining);
        assertEquals(50, localOrder.phantomVolume);
    }

    @Test
    void testAddLocalOrderAtNewLevel() {
        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertEquals(1, book.localBidOrders().size());
        assertTrue(book.bids().containsKey(100L));
        assertEquals(0, book.localBidOrders().get("BID_1").phantomVolume);
    }

    @Test
    void testDuplicateClientOidThrows() {
        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertThrows(IllegalArgumentException.class, () -> book.addLocalOrder(order));
    }

    @Test
    void testCancelOrderBid() {
        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertTrue(book.cancelOrder("BID_1"));
        assertFalse(book.localBidOrders().containsKey("BID_1"));
    }

    @Test
    void testCancelNonExistentOrder() {
        assertFalse(book.cancelOrder("NONEXISTENT"));
    }

    @Test
    void testOnTradeFilledLocalAsk() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 40));
        book.onMarketUpdate(levels);

        BacktestOrder askOrder = makeOrder(102, 8, Side.Ask, "ASK_1");
        book.addLocalOrder(askOrder);

        // Trade: buy 50 at 102. phantom=40, trade=50 → remainingVolume=10 → fills min(8,10)=8
        List<LocalOrderFill> fills = book.onTrade(102, 50, Side.Bid);
        assertFalse(fills.isEmpty());
        assertEquals("ASK_1", fills.get(0).localOrder().order.clientOid());
        assertEquals(8, fills.get(0).fillSize());
        assertFalse(book.localAskOrders().containsKey("ASK_1"));
    }

    @Test
    void testOnTradeNoLocalOrders() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 40));
        book.onMarketUpdate(levels);

        List<LocalOrderFill> fills = book.onTrade(102, 10, Side.Bid);
        assertTrue(fills.isEmpty());
    }

    @Test
    void testGetMatchingOrdersBuy() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        // Market buy, should match against all asks starting at 101
        BacktestOrder order = makeOrder(0, 60, Side.Bid, "BUY_1");
        BacktestOrder marketOrder =
                new BacktestOrder(1, 1, "BUY_1", Side.Bid, 0, 60, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderMatch> matches = book.getMatchingOrders(marketOrder);

        assertEquals(2, matches.size());
        assertEquals(101, matches.get(0).price());
        assertEquals(40, matches.get(0).size());
        assertEquals(102, matches.get(1).price());
        assertEquals(20, matches.get(1).size());
    }

    @Test
    void testGetMatchingOrdersLimitBuyPriceRestriction() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 103, 35));
        book.onMarketUpdate(levels);

        // Limit buy at 101: should only match the ask at 101, not 103
        BacktestOrder limitOrder = makeOrder(101, 50, Side.Bid, "LIMIT_BUY");
        List<OrderMatch> matches = book.getMatchingOrders(limitOrder);

        assertEquals(1, matches.size());
        assertEquals(101, matches.get(0).price());
        assertEquals(40, matches.get(0).size());
    }

    @Test
    void testSelfFillingThrows() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        // Place a local ask at 101
        BacktestOrder askOrder = makeOrder(101, 10, Side.Ask, "ASK_1");
        book.addLocalOrder(askOrder);

        // Try to buy against 101 where we have a local order — should throw
        BacktestOrder buyOrder =
                new BacktestOrder(1, 1, "BUY_1", Side.Bid, 0, 10, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        assertThrows(IllegalStateException.class, () -> book.getMatchingOrders(buyOrder));
    }

    @Test
    void testMarketUpdateRemovesLevel() {
        List<BidAskLevel> initial = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(initial);

        // Update that removes level 99 and 102 (not present in new snapshot)
        List<BidAskLevel> update = List.of(makeBidAskLevel(100, 45, 101, 38));
        book.onMarketUpdate(update);

        assertFalse(book.bids().containsKey(99L));
        assertFalse(book.asks().containsKey(102L));
        assertEquals(45, book.bids().get(100L).size);
        assertEquals(38, book.asks().get(101L).size);
    }

    @Test
    void testLifecycle() {
        // Set up initial book
        List<BidAskLevel> levels = List.of(
                makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35), makeBidAskLevel(98, 25, 103, 30));
        book.onMarketUpdate(levels);

        assertEquals(100L, book.getBestBid());
        assertEquals(101L, book.getBestAsk());

        // Add local orders
        book.addLocalOrder(makeOrder(99, 10, Side.Bid, "BID_1"));
        book.addLocalOrder(makeOrder(98, 15, Side.Bid, "BID_2"));
        book.addLocalOrder(makeOrder(102, 8, Side.Ask, "ASK_1"));

        assertEquals(2, book.localBidOrders().size());
        assertEquals(1, book.localAskOrders().size());

        // Aggressive buy trades that hit our ASK_1 at 102.
        // Level 101 (size=40) is consumed first. Then at 102: phantom=35, remaining=40 → fills min(8,5)=5
        List<LocalOrderFill> fills = book.onTrade(102, 80, Side.Bid);

        assertFalse(fills.isEmpty());

        // Cancel BID_1
        assertTrue(book.cancelOrder("BID_1"));
        assertFalse(book.localBidOrders().containsKey("BID_1"));
        assertEquals(1, book.localBidOrders().size());
    }
}
