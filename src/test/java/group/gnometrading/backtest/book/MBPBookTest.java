package group.gnometrading.backtest.book;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Order;
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

    static Order makeOrder(long price, long size, Side side, long clientOid) {
        Order order = new Order();
        order.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(price)
                .size(size)
                .side(side)
                .orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(clientOid, 0);
        return order;
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

        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertEquals(1, book.localBidOrders().size());
        assertTrue(book.localBidOrders().containsKey(1L));
        LocalOrder localOrder = book.localBidOrders().get(1L);
        assertEquals(10, localOrder.remaining);
        assertEquals(50, localOrder.phantomVolume);
    }

    @Test
    void testAddLocalOrderAtNewLevel() {
        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertEquals(1, book.localBidOrders().size());
        assertTrue(book.bids().containsKey(100L));
        assertEquals(0, book.localBidOrders().get(1L).phantomVolume);
    }

    @Test
    void testDuplicateClientOidThrows() {
        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertThrows(IllegalArgumentException.class, () -> book.addLocalOrder(order));
    }

    @Test
    void testCancelOrderBid() {
        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertTrue(book.cancelOrder(1L));
        assertFalse(book.localBidOrders().containsKey(1L));
    }

    @Test
    void testCancelNonExistentOrder() {
        assertFalse(book.cancelOrder(999L));
    }

    @Test
    void testOnTradeFilledLocalAsk() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 40));
        book.onMarketUpdate(levels);

        Order askOrder = makeOrder(102, 8, Side.Ask, 1L);
        book.addLocalOrder(askOrder);

        // Trade: buy 50 at 102. phantom=40, trade=50 → remainingVolume=10 → fills min(8,10)=8
        List<LocalOrderFill> fills = book.onTrade(102, 50, Side.Bid);
        assertFalse(fills.isEmpty());
        assertEquals(1L, fills.get(0).localOrder().order.getClientOidCounter());
        assertEquals(8, fills.get(0).fillSize());
        assertFalse(book.localAskOrders().containsKey(1L));
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
        Order marketOrder = new Order();
        marketOrder
                .encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(0)
                .size(60)
                .side(Side.Bid)
                .orderType(OrderType.MARKET)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        marketOrder.encodeClientOid(1L, 0);
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
        Order limitOrder = makeOrder(101, 50, Side.Bid, 1L);
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
        Order askOrder = makeOrder(101, 10, Side.Ask, 1L);
        book.addLocalOrder(askOrder);

        // Try to buy against 101 where we have a local order — returns empty (self-trade prevented)
        Order buyOrder = new Order();
        buyOrder.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(0)
                .size(10)
                .side(Side.Bid)
                .orderType(OrderType.MARKET)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        buyOrder.encodeClientOid(2L, 0);
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
        book.addLocalOrder(makeOrder(99, 10, Side.Bid, 1L));
        book.addLocalOrder(makeOrder(98, 15, Side.Bid, 2L));
        book.addLocalOrder(makeOrder(102, 8, Side.Ask, 3L));

        assertEquals(2, book.localBidOrders().size());
        assertEquals(1, book.localAskOrders().size());

        // Aggressive buy trades that hit our ASK at 102.
        // Level 101 (size=40) is consumed first. Then at 102: phantom=35, remaining=40 → fills min(8,5)=5
        List<LocalOrderFill> fills = book.onTrade(102, 80, Side.Bid);

        assertFalse(fills.isEmpty());

        // Cancel bid 1
        assertTrue(book.cancelOrder(1L));
        assertFalse(book.localBidOrders().containsKey(1L));
        assertEquals(1, book.localBidOrders().size());
    }

    // --- Ask-side local order tests ---

    @Test
    void testAddLocalOrderAsk() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(101, 10, Side.Ask, 1L);
        book.addLocalOrder(order);

        assertEquals(1, book.localAskOrders().size());
        assertTrue(book.localBidOrders().isEmpty());
        assertTrue(book.localAskOrders().containsKey(1L));
        LocalOrder localOrder = book.localAskOrders().get(1L);
        assertEquals(10, localOrder.remaining);
        assertEquals(40, localOrder.phantomVolume);
    }

    @Test
    void testAddLocalOrderAskAtNewLevel() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(105, 10, Side.Ask, 1L);
        book.addLocalOrder(order);

        assertTrue(book.asks().containsKey(105L));
        assertEquals(0, book.localAskOrders().get(1L).phantomVolume);
    }

    @Test
    void testCancelOrderAsk() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(101, 10, Side.Ask, 1L);
        book.addLocalOrder(order);

        assertTrue(book.cancelOrder(1L));
        assertTrue(book.localAskOrders().isEmpty());
        assertTrue(book.asks().containsKey(101L));
        assertFalse(book.asks().get(101L).hasLocalOrders());
    }

    // --- Modify order tests ---

    @Test
    void testModifyLocalOrderPriceChange() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertTrue(book.modifyLocalOrder(1L, 99, 10));

        LocalOrder amended = book.localBidOrders().get(1L);
        assertEquals(99, amended.order.decoder.price());
        assertEquals(10, amended.remaining);
        assertEquals(30, amended.phantomVolume);
        assertFalse(book.bids().get(100L).hasLocalOrders());
        assertTrue(book.bids().get(99L).hasLocalOrders());
    }

    @Test
    void testModifyLocalOrderSizeIncrease() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertTrue(book.modifyLocalOrder(1L, 100, 15));

        LocalOrder amended = book.localBidOrders().get(1L);
        assertEquals(15, amended.order.decoder.size());
        assertEquals(15, amended.remaining);
        assertEquals(50, amended.phantomVolume);
    }

    @Test
    void testModifyLocalOrderSizeDecrease() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertTrue(book.modifyLocalOrder(1L, 100, 6));

        LocalOrder amended = book.localBidOrders().get(1L);
        assertEquals(6, amended.order.decoder.size());
        assertEquals(6, amended.remaining);
        assertEquals(50, amended.phantomVolume);
    }

    @Test
    void testModifyLocalOrderSizeDecreaseBelowRemaining() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        // Order size=10, but after a partial fill remaining would be 3
        // Simulate by placing with explicit remaining
        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order, 3);

        // Modify size to 2 (sizeDiff = 2-10 = -8, remaining = max(0, 3 + (-8)) = 0)
        assertTrue(book.modifyLocalOrder(1L, 100, 2));

        LocalOrder amended = book.localBidOrders().get(1L);
        assertEquals(2, amended.order.decoder.size());
        assertEquals(0, amended.remaining);
    }

    @Test
    void testModifyNonExistentOrder() {
        assertFalse(book.modifyLocalOrder(999L, 100, 10));
    }

    @Test
    void testModifyLocalOrderPriceChangeToNewLevel() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertTrue(book.modifyLocalOrder(1L, 97, 10));

        assertTrue(book.bids().containsKey(97L));
        assertEquals(0, book.localBidOrders().get(1L).phantomVolume);
    }

    @Test
    void testModifyLocalOrderPriceChangeRemovesStaleOldLevel() {
        // Local bid at a level with no market depth (size=0) — modifying away should remove it
        Order order = makeOrder(100, 10, Side.Bid, 1L);
        book.addLocalOrder(order);

        assertTrue(book.bids().containsKey(100L));

        assertTrue(book.modifyLocalOrder(1L, 99, 10));

        // Old level at 100 (size=0, no local orders) should be cleaned up
        assertFalse(book.bids().containsKey(100L));
    }

    @Test
    void testCancelOrderRemovesStaleLevel() {
        // Local ask at a level with no market depth — cancel should remove the level
        Order order = makeOrder(105, 10, Side.Ask, 1L);
        book.addLocalOrder(order);
        assertTrue(book.asks().containsKey(105L));

        assertTrue(book.cancelOrder(1L));

        assertFalse(book.asks().containsKey(105L));
    }

    @Test
    void testCancelOrderKeepsLevelIfMarketDepthExists() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40));
        book.onMarketUpdate(levels);

        Order order = makeOrder(101, 10, Side.Ask, 1L);
        book.addLocalOrder(order);

        assertTrue(book.cancelOrder(1L));

        // Level at 101 still has market depth (size=40), so it should remain
        assertTrue(book.asks().containsKey(101L));
        assertEquals(40, book.asks().get(101L).size);
    }

    // --- Market update with local orders ---

    @Test
    void testOnMarketUpdateKeepsLevelAliveForLocalOrders() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        book.addLocalOrder(makeOrder(99, 5, Side.Bid, 1L));

        // New update omits level 99 and 102
        List<BidAskLevel> update = List.of(makeBidAskLevel(100, 45, 101, 38));
        book.onMarketUpdate(update);

        assertTrue(book.bids().containsKey(99L));
        assertEquals(0, book.bids().get(99L).size);
        assertTrue(book.bids().get(99L).hasLocalOrders());
        assertTrue(book.localBidOrders().containsKey(1L));
    }

    @Test
    void testOnMarketUpdateCrossedBookFillsBidLocalOrders() {
        // Seed book first so the local bid gets a non-zero phantom
        List<BidAskLevel> seed = List.of(makeBidAskLevel(103, 50, 110, 40));
        book.onMarketUpdate(seed);

        // Local bid at 103 — phantom=50 (the existing bid depth)
        Order order = makeOrder(103, 8, Side.Bid, 1L);
        book.addLocalOrder(order);
        assertEquals(50, book.localBidOrders().get(1L).phantomVolume);

        // Market update: ask drops to 103, crossing the book (bestBid=103, bestAsk=103).
        // Phantom should be bypassed, so bid fills immediately despite phantom=50.
        List<BidAskLevel> crossed = List.of(makeBidAskLevel(103, 50, 103, 30));
        List<LocalOrderFill> fills = book.onMarketUpdate(crossed);

        assertFalse(fills.isEmpty());
        assertEquals(1L, fills.get(0).localOrder().order.getClientOidCounter());
        assertEquals(8, fills.get(0).fillSize());
        assertFalse(book.localBidOrders().containsKey(1L));
    }

    @Test
    void testOnMarketUpdateCrossedBookFillsAskLocalOrders() {
        // Seed book first so the local ask gets a non-zero phantom
        List<BidAskLevel> seed = List.of(makeBidAskLevel(90, 30, 98, 40));
        book.onMarketUpdate(seed);

        // Local ask at 98 — phantom=40 (the existing ask depth)
        Order order = makeOrder(98, 8, Side.Ask, 1L);
        book.addLocalOrder(order);
        assertEquals(40, book.localAskOrders().get(1L).phantomVolume);

        // Market update: bid rises to 98, crossing the book (bestBid=98, bestAsk=98).
        // Phantom should be bypassed, so ask fills immediately despite phantom=40.
        List<BidAskLevel> crossed = List.of(makeBidAskLevel(98, 40, 98, 40));
        List<LocalOrderFill> fills = book.onMarketUpdate(crossed);

        assertFalse(fills.isEmpty());
        assertEquals(1L, fills.get(0).localOrder().order.getClientOidCounter());
        assertEquals(8, fills.get(0).fillSize());
        assertFalse(book.localAskOrders().containsKey(1L));
    }

    // --- Trade edge cases ---

    @Test
    void testOnTradeMultipleLocalOrdersSameLevel() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 5));
        book.onMarketUpdate(levels);

        // Both orders placed at 102 with phantom=5
        book.addLocalOrder(makeOrder(102, 4, Side.Ask, 1L));
        book.addLocalOrder(makeOrder(102, 6, Side.Ask, 2L));

        // Trade 20 at 102: phantom=5, remaining=15. Fill order 1 (4), then order 2 (6)
        List<LocalOrderFill> fills = book.onTrade(102, 20, Side.Bid);

        assertEquals(2, fills.size());
        assertEquals(1L, fills.get(0).localOrder().order.getClientOidCounter());
        assertEquals(4, fills.get(0).fillSize());
        assertEquals(2L, fills.get(1).localOrder().order.getClientOidCounter());
        assertEquals(6, fills.get(1).fillSize());
        assertTrue(book.localAskOrders().isEmpty());
    }

    @Test
    void testOnTradeAcrossMultipleLevels() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 10, 101, 40), makeBidAskLevel(99, 20, 102, 35));
        book.onMarketUpdate(levels);

        // Local bid at 100 (phantom=10) and 99 (phantom=20)
        book.addLocalOrder(makeOrder(100, 5, Side.Bid, 1L));
        book.addLocalOrder(makeOrder(99, 3, Side.Bid, 2L));

        // Sell trade down to 99: walks bids descending
        // At 100: onTrade(40, deque): phantom=10, remainingVol=40-10=30, fill min(5,30)=5
        //   filledQty=5, remainingSize=40-5=35, consume level.size=min(35,10)=10, remainingSize=25
        // At 99: onTrade(25, deque): phantom=20, remainingVol=25-20=5, fill min(3,5)=3
        List<LocalOrderFill> fills = book.onTrade(99, 40, Side.Ask);

        assertEquals(2, fills.size());
        assertEquals(1L, fills.get(0).localOrder().order.getClientOidCounter());
        assertEquals(5, fills.get(0).fillSize());
        assertEquals(2L, fills.get(1).localOrder().order.getClientOidCounter());
        assertEquals(3, fills.get(1).fillSize());
        assertTrue(book.localBidOrders().isEmpty());
    }

    @Test
    void testOnTradePartialFillSingleOrder() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 102, 3));
        book.onMarketUpdate(levels);

        book.addLocalOrder(makeOrder(102, 10, Side.Ask, 1L));

        // Trade 5 at 102: phantom=3, remainingVol=5-3=2, fill min(10,2)=2
        List<LocalOrderFill> fills = book.onTrade(102, 5, Side.Bid);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).fillSize());
        assertEquals(8, book.localAskOrders().get(1L).remaining);
        assertTrue(book.localAskOrders().containsKey(1L));
    }

    @Test
    void testOnTradeDoesNotFillBeyondTradePrice() {
        List<BidAskLevel> levels = List.of(
                makeBidAskLevel(100, 50, 101, 10), makeBidAskLevel(99, 30, 102, 20), makeBidAskLevel(98, 20, 103, 30));
        book.onMarketUpdate(levels);

        // Local ask at 101 (phantom=10) and 103 (phantom=30)
        book.addLocalOrder(makeOrder(101, 5, Side.Ask, 1L));
        book.addLocalOrder(makeOrder(103, 5, Side.Ask, 2L));

        // Trade up to 102: should process 101 (<=102) but NOT 103 (>102)
        List<LocalOrderFill> fills = book.onTrade(102, 50, Side.Bid);

        assertEquals(1, fills.size());
        assertEquals(1L, fills.get(0).localOrder().order.getClientOidCounter());
        assertTrue(book.localAskOrders().containsKey(2L));
    }

    // --- Matching edge cases ---

    @Test
    void testGetMatchingOrdersSell() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 40), makeBidAskLevel(99, 30, 102, 35));
        book.onMarketUpdate(levels);

        Order order = new Order();
        order.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(0)
                .size(60)
                .side(Side.Ask)
                .orderType(OrderType.MARKET)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(1L, 0);
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
        Order order = makeOrder(99, 60, Side.Ask, 1L);
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertEquals(1, matches.size());
        assertEquals(100, matches.get(0).price());
        assertEquals(50, matches.get(0).size());
    }

    @Test
    void testGetMatchingOrdersEmptyBook() {
        Order order = new Order();
        order.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(0)
                .size(10)
                .side(Side.Bid)
                .orderType(OrderType.MARKET)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(1L, 0);
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertTrue(matches.isEmpty());
    }

    @Test
    void testGetMatchingOrdersPartialFillInsufficientLiquidity() {
        List<BidAskLevel> levels = List.of(makeBidAskLevel(100, 50, 101, 20));
        book.onMarketUpdate(levels);

        Order order = new Order();
        order.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(0)
                .size(50)
                .side(Side.Bid)
                .orderType(OrderType.MARKET)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(1L, 0);
        List<OrderMatch> matches = book.getMatchingOrders(order);

        assertEquals(1, matches.size());
        assertEquals(101, matches.get(0).price());
        assertEquals(20, matches.get(0).size());
    }
}
