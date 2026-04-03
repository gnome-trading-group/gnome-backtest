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
    void testSelfFillingReturnsEmpty() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        // Place a local ask at 101
        BacktestOrder askOrder = makeOrder(101, 10, Side.Ask, "ASK_1");
        book.addLocalOrder(askOrder);

        // Try to buy against 101 where we have a local order — returns empty (self-trade prevented)
        BacktestOrder buyOrder =
                new BacktestOrder(1, 1, "BUY_1", Side.Bid, 0, 10, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderMatch> matches = book.getMatchingOrders(buyOrder);
        assertTrue(matches.isEmpty());
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

    // --- Ask-side local order tests ---

    @Test
    void testAddLocalOrderAsk() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(101, 10, Side.Ask, "ASK_1");
        book.addLocalOrder(order);

        assertEquals(1, book.localAskOrders().size());
        assertTrue(book.localBidOrders().isEmpty());
        assertTrue(book.localAskOrders().containsKey("ASK_1"));
        LocalOrder localOrder = book.localAskOrders().get("ASK_1");
        assertEquals(10, localOrder.remaining);
        assertEquals(40, localOrder.phantomVolume);
    }

    @Test
    void testAddLocalOrderAskAtNewLevel() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(105, 10, Side.Ask, "ASK_NEW");
        book.addLocalOrder(order);

        assertTrue(book.asks().containsKey(105L));
        assertEquals(0, book.localAskOrders().get("ASK_NEW").phantomVolume);
    }

    @Test
    void testCancelOrderAsk() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(101, 10, Side.Ask, "ASK_1");
        book.addLocalOrder(order);

        assertTrue(book.cancelOrder("ASK_1"));
        assertTrue(book.localAskOrders().isEmpty());
        assertTrue(book.asks().containsKey(101L));
        assertFalse(book.asks().get(101L).hasLocalOrders());
    }

    // --- Amend order tests ---

    @Test
    void testAmendLocalOrderPriceChange() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertTrue(book.amendLocalOrder("BID_1", 99, 10));

        LocalOrder amended = book.localBidOrders().get("BID_1");
        assertEquals(99, amended.order.price());
        assertEquals(10, amended.remaining);
        assertEquals(30, amended.phantomVolume);
        assertFalse(book.bids().get(100L).hasLocalOrders());
        assertTrue(book.bids().get(99L).hasLocalOrders());
    }

    @Test
    void testAmendLocalOrderSizeIncrease() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertTrue(book.amendLocalOrder("BID_1", 100, 15));

        LocalOrder amended = book.localBidOrders().get("BID_1");
        assertEquals(15, amended.order.size());
        assertEquals(15, amended.remaining);
        assertEquals(50, amended.phantomVolume);
    }

    @Test
    void testAmendLocalOrderSizeDecrease() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertTrue(book.amendLocalOrder("BID_1", 100, 6));

        LocalOrder amended = book.localBidOrders().get("BID_1");
        assertEquals(6, amended.order.size());
        assertEquals(6, amended.remaining);
        assertEquals(50, amended.phantomVolume);
    }

    @Test
    void testAmendLocalOrderSizeDecreaseBelowRemaining() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        // Order size=10, but after a partial fill remaining would be 3
        // Simulate by placing with explicit remaining
        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order, 3);

        // Amend size to 2 (sizeDiff = 2-10 = -8, remaining = max(0, 3 + (-8)) = 0)
        assertTrue(book.amendLocalOrder("BID_1", 100, 2));

        LocalOrder amended = book.localBidOrders().get("BID_1");
        assertEquals(2, amended.order.size());
        assertEquals(0, amended.remaining);
    }

    @Test
    void testAmendNonExistentOrder() {
        assertFalse(book.amendLocalOrder("NONEXISTENT", 100, 10));
    }

    @Test
    void testAmendLocalOrderPriceChangeToNewLevel() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertTrue(book.amendLocalOrder("BID_1", 97, 10));

        assertTrue(book.bids().containsKey(97L));
        assertEquals(0, book.localBidOrders().get("BID_1").phantomVolume);
    }

    @Test
    void testAmendLocalOrderPriceChangeRemovesStaleOldLevel() {
        // Local bid at a level with no market depth (size=0) — amending away should remove it
        BacktestOrder order = makeOrder(100, 10, Side.Bid, "BID_1");
        book.addLocalOrder(order);

        assertTrue(book.bids().containsKey(100L));

        assertTrue(book.amendLocalOrder("BID_1", 99, 10));

        // Old level at 100 (size=0, no local orders) should be cleaned up
        assertFalse(book.bids().containsKey(100L));
    }

    @Test
    void testCancelOrderRemovesStaleLevel() {
        // Local ask at a level with no market depth — cancel should remove the level
        BacktestOrder order = makeOrder(105, 10, Side.Ask, "ASK_1");
        book.addLocalOrder(order);
        assertTrue(book.asks().containsKey(105L));

        assertTrue(book.cancelOrder("ASK_1"));

        assertFalse(book.asks().containsKey(105L));
    }

    @Test
    void testCancelOrderKeepsLevelIfMarketDepthExists() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        BacktestOrder order = makeOrder(101, 10, Side.Ask, "ASK_1");
        book.addLocalOrder(order);

        assertTrue(book.cancelOrder("ASK_1"));

        // Level at 101 still has market depth (size=40), so it should remain
        assertTrue(book.asks().containsKey(101L));
        assertEquals(40, book.asks().get(101L).size);
    }

    // --- Market update with local orders ---

    @Test
    void testOnMarketUpdateKeepsLevelAliveForLocalOrders() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        book.addLocalOrder(makeOrder(99, 5, Side.Bid, "BID_1"));

        // New update omits level 99 and 102
        List<BidAskLevel> update = List.of(makeBidAskLevel(100, 45, 101, 38));
        book.onMarketUpdate(update);

        assertTrue(book.bids().containsKey(99L));
        assertEquals(0, book.bids().get(99L).size);
        assertTrue(book.bids().get(99L).hasLocalOrders());
        assertTrue(book.localBidOrders().containsKey("BID_1"));
    }

    @Test
    void testOnMarketUpdateCrossedBookFillsBidLocalOrders() {
        // Seed book first so the local bid gets a non-zero phantom
        List<BidAskLevel> seed = List.of(makeBidAskLevel(103, 50, 110, 40));
        book.onMarketUpdate(seed);

        // Local bid at 103 — phantom=50 (the existing bid depth)
        BacktestOrder order = makeOrder(103, 8, Side.Bid, "BID_1");
        book.addLocalOrder(order);
        assertEquals(50, book.localBidOrders().get("BID_1").phantomVolume);

        // Market update: ask drops to 103, crossing the book (bestBid=103, bestAsk=103).
        // Phantom should be bypassed, so BID_1 fills immediately despite phantom=50.
        List<BidAskLevel> levels = List.of(makeBidAskLevel(103, 50, 103, 30));
        List<LocalOrderFill> fills = book.onMarketUpdate(levels);

        assertFalse(fills.isEmpty());
        assertEquals("BID_1", fills.get(0).localOrder().order.clientOid());
        assertEquals(8, fills.get(0).fillSize());
        assertFalse(book.localBidOrders().containsKey("BID_1"));
    }

    @Test
    void testOnMarketUpdateCrossedBookFillsAskLocalOrders() {
        // Seed book first so the local ask gets a non-zero phantom
        List<BidAskLevel> seed = List.of(makeBidAskLevel(90, 30, 98, 40));
        book.onMarketUpdate(seed);

        // Local ask at 98 — phantom=40 (the existing ask depth)
        BacktestOrder order = makeOrder(98, 8, Side.Ask, "ASK_1");
        book.addLocalOrder(order);
        assertEquals(40, book.localAskOrders().get("ASK_1").phantomVolume);

        // Market update: bid rises to 98, crossing the book (bestBid=98, bestAsk=98).
        // Phantom should be bypassed, so ASK_1 fills immediately despite phantom=40.
        List<BidAskLevel> levels = List.of(makeBidAskLevel(98, 40, 98, 40));
        List<LocalOrderFill> fills = book.onMarketUpdate(levels);

        assertFalse(fills.isEmpty());
        assertEquals("ASK_1", fills.get(0).localOrder().order.clientOid());
        assertEquals(8, fills.get(0).fillSize());
        assertFalse(book.localAskOrders().containsKey("ASK_1"));
    }

    // --- Trade edge cases ---

    @Test
    void testOnTradeMultipleLocalOrdersSameLevel() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 5));
        book.onMarketUpdate(levels);

        // Both orders placed at 102 with phantom=5
        book.addLocalOrder(makeOrder(102, 4, Side.Ask, "ASK_1"));
        book.addLocalOrder(makeOrder(102, 6, Side.Ask, "ASK_2"));

        // Trade 20 at 102: phantom=5, remaining=15. Fill ASK_1(4), then ASK_2(6)
        List<LocalOrderFill> fills = book.onTrade(102, 20, Side.Bid);

        assertEquals(2, fills.size());
        assertEquals("ASK_1", fills.get(0).localOrder().order.clientOid());
        assertEquals(4, fills.get(0).fillSize());
        assertEquals("ASK_2", fills.get(1).localOrder().order.clientOid());
        assertEquals(6, fills.get(1).fillSize());
        assertTrue(book.localAskOrders().isEmpty());
    }

    @Test
    void testOnTradeAcrossMultipleLevels() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 10, 101, 40), makeBidAskLevel(99, 20, 102, 35));
        book.onMarketUpdate(levels);

        // Local bid at 100 (phantom=10) and 99 (phantom=20)
        book.addLocalOrder(makeOrder(100, 5, Side.Bid, "BID_100"));
        book.addLocalOrder(makeOrder(99, 3, Side.Bid, "BID_99"));

        // Sell trade down to 99: walks bids descending
        // At 100: onTrade(40, deque): phantom=10, remainingVol=40-10=30, fill min(5,30)=5
        //   filledQty=5, remainingSize=40-5=35, consume level.size=min(35,10)=10, remainingSize=25
        // At 99: onTrade(25, deque): phantom=20, remainingVol=25-20=5, fill min(3,5)=3
        List<LocalOrderFill> fills = book.onTrade(99, 40, Side.Ask);

        assertEquals(2, fills.size());
        assertEquals("BID_100", fills.get(0).localOrder().order.clientOid());
        assertEquals(5, fills.get(0).fillSize());
        assertEquals("BID_99", fills.get(1).localOrder().order.clientOid());
        assertEquals(3, fills.get(1).fillSize());
        assertTrue(book.localBidOrders().isEmpty());
    }

    @Test
    void testOnTradePartialFillSingleOrder() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 3));
        book.onMarketUpdate(levels);

        book.addLocalOrder(makeOrder(102, 10, Side.Ask, "ASK_1"));

        // Trade 5 at 102: phantom=3, remainingVol=5-3=2, fill min(10,2)=2
        List<LocalOrderFill> fills = book.onTrade(102, 5, Side.Bid);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).fillSize());
        assertEquals(8, book.localAskOrders().get("ASK_1").remaining);
        assertTrue(book.localAskOrders().containsKey("ASK_1"));
    }

    @Test
    void testOnTradeDoesNotFillBeyondTradePrice() {
        List<BidAskLevel> levels = List.of(
                makeBidAskLevel(100, 50, 101, 10), makeBidAskLevel(99, 30, 102, 20), makeBidAskLevel(98, 20, 103, 30));
        book.onMarketUpdate(levels);

        // Local ask at 101 (phantom=10) and 103 (phantom=30)
        book.addLocalOrder(makeOrder(101, 5, Side.Ask, "ASK_101"));
        book.addLocalOrder(makeOrder(103, 5, Side.Ask, "ASK_103"));

        // Trade up to 102: should process 101 (<=102) but NOT 103 (>102)
        List<LocalOrderFill> fills = book.onTrade(102, 50, Side.Bid);

        assertEquals(1, fills.size());
        assertEquals("ASK_101", fills.get(0).localOrder().order.clientOid());
        assertTrue(book.localAskOrders().containsKey("ASK_103"));
    }

    // --- Matching edge cases ---

    @Test
    void testGetMatchingOrdersSell() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        BacktestOrder order =
                new BacktestOrder(1, 1, "SELL_1", Side.Ask, 0, 60, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertEquals(2, matches.size());
        assertEquals(100, matches.get(0).price());
        assertEquals(50, matches.get(0).size());
        assertEquals(99, matches.get(1).price());
        assertEquals(10, matches.get(1).size());
    }

    @Test
    void testGetMatchingOrdersLimitSellPriceRestriction() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(98, 30, 102, 35));
        book.onMarketUpdate(levels);

        // Limit sell at 99: should only match bids >= 99, which is only bid at 100
        BacktestOrder order = makeOrder(99, 60, Side.Ask, "LIMIT_SELL");
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertEquals(1, matches.size());
        assertEquals(100, matches.get(0).price());
        assertEquals(50, matches.get(0).size());
    }

    @Test
    void testGetMatchingOrdersEmptyBook() {
        BacktestOrder order =
                new BacktestOrder(1, 1, "BUY_1", Side.Bid, 0, 10, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertTrue(matches.isEmpty());
    }

    @Test
    void testGetMatchingOrdersPartialFillInsufficientLiquidity() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 20));
        book.onMarketUpdate(levels);

        BacktestOrder order =
                new BacktestOrder(1, 1, "BUY_1", Side.Bid, 0, 50, OrderType.MARKET, TimeInForce.GOOD_TILL_CANCELED);
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertEquals(1, matches.size());
        assertEquals(101, matches.get(0).price());
        assertEquals(20, matches.get(0).size());
    }
}
