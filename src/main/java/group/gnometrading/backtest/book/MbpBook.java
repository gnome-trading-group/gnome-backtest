package group.gnometrading.backtest.book;

import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class MbpBook {

    private static final long PRICE_NULL = Long.MIN_VALUE;

    private final QueueModel queueModel;

    // TreeMap bids: descending order (best bid first), asks: ascending (best ask first)
    private final TreeMap<Long, OrderBookLevel> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Long, OrderBookLevel> asks = new TreeMap<>();

    // local_orders[side][clientOidCounter] -> LocalOrder
    private final Map<Long, LocalOrder> localBidOrders = new HashMap<>();
    private final Map<Long, LocalOrder> localAskOrders = new HashMap<>();

    public MbpBook(QueueModel queueModel) {
        this.queueModel = queueModel;
    }

    public Long getBestBid() {
        return bids.isEmpty() ? null : bids.firstKey();
    }

    public Long getBestAsk() {
        return asks.isEmpty() ? null : asks.firstKey();
    }

    /**
     * Reconciles the book against the provided MBP levels, adjusting phantom volumes via the queue model.
     * Returns fills if any local orders were crossed after the update.
     */
    public List<LocalOrderFill> onMarketUpdate(List<BidAskLevel> levels) {
        Map<Long, Long> currBids = new HashMap<>();
        Map<Long, Long> currAsks = new HashMap<>();
        for (BidAskLevel level : levels) {
            if (level.bidPrice() != PRICE_NULL) {
                currBids.put(level.bidPrice(), level.bidSize());
            }
            if (level.askPrice() != PRICE_NULL) {
                currAsks.put(level.askPrice(), level.askSize());
            }
        }

        reconcileSide(bids, currBids);
        reconcileSide(asks, currAsks);

        if (localBidOrders.isEmpty() && localAskOrders.isEmpty()) {
            return Collections.emptyList();
        }

        Long bestBid = getBestBid();
        Long bestAsk = getBestAsk();
        if (bestBid == null || bestAsk == null || bestBid < bestAsk) {
            return Collections.emptyList();
        }

        // Book is crossed — check for fills
        List<LocalOrderFill> allFills = new ArrayList<>();
        checkBidFills(bestAsk, allFills);
        checkAskFills(bestBid, allFills);

        clearFills(allFills);
        return allFills;
    }

    private void checkBidFills(Long bestAsk, List<LocalOrderFill> allFills) {
        for (long bidPrice : bids.keySet()) {
            if (bidPrice < bestAsk) {
                break;
            }
            OrderBookLevel bidLevel = bids.get(bidPrice);
            if (bidLevel == null || !bidLevel.hasLocalOrders()) {
                continue;
            }
            bypassPhantom(bidLevel.localOrders);
            long remainingToFill =
                    bidLevel.localOrders.stream().mapToLong(lo -> lo.remaining).sum();
            for (long askPrice : asks.keySet()) {
                if (askPrice > bidPrice || remainingToFill == 0) {
                    break;
                }
                OrderBookLevel askLevel = asks.get(askPrice);
                if (askLevel == null || askLevel.size == 0) {
                    continue;
                }
                long tradeSize = Math.min(remainingToFill, askLevel.size);
                List<LocalOrderFill> fills = queueModel.onTrade(tradeSize, bidLevel.localOrders);
                allFills.addAll(fills);
                long filledQty =
                        fills.stream().mapToLong(LocalOrderFill::fillSize).sum();
                remainingToFill -= filledQty;
                askLevel.size -= filledQty;
            }
        }
    }

    private void checkAskFills(Long bestBid, List<LocalOrderFill> allFills) {
        for (long askPrice : asks.keySet()) {
            if (askPrice > bestBid) {
                break;
            }
            OrderBookLevel askLevel = asks.get(askPrice);
            if (askLevel == null || !askLevel.hasLocalOrders()) {
                continue;
            }
            bypassPhantom(askLevel.localOrders);
            long remainingToFill =
                    askLevel.localOrders.stream().mapToLong(lo -> lo.remaining).sum();
            for (long bidPrice : bids.keySet()) {
                if (bidPrice < askPrice || remainingToFill == 0) {
                    break;
                }
                OrderBookLevel bidLevel = bids.get(bidPrice);
                if (bidLevel == null || bidLevel.size == 0) {
                    continue;
                }
                long tradeSize = Math.min(remainingToFill, bidLevel.size);
                List<LocalOrderFill> fills = queueModel.onTrade(tradeSize, askLevel.localOrders);
                allFills.addAll(fills);
                long filledQty =
                        fills.stream().mapToLong(LocalOrderFill::fillSize).sum();
                remainingToFill -= filledQty;
                bidLevel.size -= filledQty;
            }
        }
    }

    private void bypassPhantom(ArrayDeque<LocalOrder> localOrders) {
        for (LocalOrder lo : localOrders) {
            lo.phantomVolume = 0;
        }
    }

    /**
     * Processes a trade from the market feed, filling local orders on the opposite side.
     */
    public List<LocalOrderFill> onTrade(long price, long size, Side tradeSide) {
        if (localBidOrders.isEmpty() && localAskOrders.isEmpty()) {
            return Collections.emptyList();
        }

        // The trade side is the aggressor side; our local orders are on the opposite side
        boolean tradeIsBid = tradeSide == Side.Bid;
        TreeMap<Long, OrderBookLevel> oppBook = tradeIsBid ? asks : bids;
        List<LocalOrderFill> allFills = new ArrayList<>();

        long remainingSize = size;
        for (Map.Entry<Long, OrderBookLevel> entry : oppBook.entrySet()) {
            if (remainingSize <= 0) {
                break;
            }
            long levelPrice = entry.getKey();
            // For ask levels: stop if ask price > trade price (trade can't reach here)
            // For bid levels: stop if bid price < trade price
            if (tradeIsBid ? levelPrice > price : levelPrice < price) {
                break;
            }

            OrderBookLevel level = entry.getValue();
            if (level == null) {
                throw new IllegalStateException("Malformed local book: null level at price " + levelPrice);
            }

            List<LocalOrderFill> fills = queueModel.onTrade(remainingSize, level.localOrders);
            allFills.addAll(fills);
            long filledQty = fills.stream().mapToLong(LocalOrderFill::fillSize).sum();
            remainingSize -= filledQty;

            // Consume remaining market (non-local) volume at this level
            long leftToConsume = Math.min(remainingSize, level.size);
            remainingSize -= leftToConsume;
            level.size -= leftToConsume;
        }

        clearFills(allFills);
        return allFills;
    }

    /**
     * Places a local order into the book with phantom volume equal to the current displayed depth at the price level.
     */
    public void addLocalOrder(Order order, long remaining) {
        Side side = order.decoder.side();
        long price = order.decoder.price();
        long clientOid = order.getClientOidCounter();
        TreeMap<Long, OrderBookLevel> book = side == Side.Bid ? bids : asks;
        Map<Long, LocalOrder> localOrders = side == Side.Bid ? localBidOrders : localAskOrders;

        if (localOrders.containsKey(clientOid)) {
            throw new IllegalArgumentException("Duplicate client OID: " + clientOid);
        }

        OrderBookLevel level = book.get(price);
        if (level == null) {
            level = new OrderBookLevel(price, 0);
            book.put(price, level);
        }

        LocalOrder localOrder = new LocalOrder(order, remaining, level.size);
        localOrders.put(clientOid, localOrder);
        level.localOrders.addLast(localOrder);
    }

    public void addLocalOrder(Order order) {
        addLocalOrder(order, order.decoder.size());
    }

    /**
     * Cancels a local order by clientOidCounter. Returns true if the order was found and removed.
     */
    public boolean cancelOrder(long clientOid) {
        LocalOrder localOrder = localBidOrders.remove(clientOid);
        Side side = Side.Bid;
        if (localOrder == null) {
            localOrder = localAskOrders.remove(clientOid);
            side = Side.Ask;
        }
        if (localOrder == null) {
            return false;
        }

        TreeMap<Long, OrderBookLevel> book = side == Side.Bid ? bids : asks;
        long price = localOrder.order.decoder.price();
        OrderBookLevel level = book.get(price);
        if (level != null) {
            level.localOrders.remove(localOrder);
            if (level.size == 0 && !level.hasLocalOrders()) {
                book.remove(price);
            }
        }
        return true;
    }

    /**
     * Modifies a local order's price and/or size. If price changes, the order moves to a new
     * level and loses queue position. If only size changes, it stays in place.
     * Returns true if the order was found and modified.
     */
    public boolean modifyLocalOrder(long clientOid, long newPrice, long newSize) {
        // Find in bid or ask maps
        LocalOrder localOrder = localBidOrders.get(clientOid);
        Side side = Side.Bid;
        if (localOrder == null) {
            localOrder = localAskOrders.get(clientOid);
            side = Side.Ask;
        }
        if (localOrder == null) {
            return false;
        }

        TreeMap<Long, OrderBookLevel> book = side == Side.Bid ? bids : asks;
        long oldPrice = localOrder.order.decoder.price();
        long oldSize = localOrder.order.decoder.size();

        if (oldPrice != newPrice) {
            // Price changed — remove from old level, add to new level (loses queue position)
            OrderBookLevel oldLevel = book.get(oldPrice);
            if (oldLevel != null) {
                oldLevel.localOrders.remove(localOrder);
                if (oldLevel.size == 0 && !oldLevel.hasLocalOrders()) {
                    book.remove(oldPrice);
                }
            }

            // Update order fields by re-encoding into the existing SBE buffer
            localOrder.order.encoder.price(newPrice).size(newSize);
            localOrder.remaining = newSize;

            // Add to new price level
            OrderBookLevel newLevel = book.get(newPrice);
            if (newLevel == null) {
                newLevel = new OrderBookLevel(newPrice, 0);
                book.put(newPrice, newLevel);
            }
            localOrder.phantomVolume = newLevel.size;
            newLevel.localOrders.addLast(localOrder);
        } else if (oldSize != newSize) {
            // Only size changed — update in place, keep queue position
            long sizeDiff = newSize - oldSize;
            localOrder.remaining = Math.max(0, localOrder.remaining + sizeDiff);
            localOrder.order.encoder.size(newSize);
        }

        return true;
    }

    /**
     * Returns immediate matches for the order by walking the opposite side of the book.
     * Skips levels with local orders to avoid self-fills.
     */
    public List<OrderMatch> getMatchingOrders(Order order) {
        List<OrderMatch> matches = new ArrayList<>();
        long remainingSize = order.decoder.size();
        long orderPrice = order.decoder.price();
        OrderType orderType = order.decoder.orderType();
        boolean isBuy = order.decoder.side() == Side.Bid;
        TreeMap<Long, OrderBookLevel> oppBook = isBuy ? asks : bids;

        for (Map.Entry<Long, OrderBookLevel> entry : oppBook.entrySet()) {
            if (remainingSize == 0) {
                break;
            }
            long levelPrice = entry.getKey();
            if (orderType == OrderType.LIMIT) {
                if (isBuy ? levelPrice > orderPrice : levelPrice < orderPrice) {
                    break;
                }
            }

            OrderBookLevel level = entry.getValue();
            if (level.size == 0) {
                continue;
            }
            if (level.hasLocalOrders()) {
                return Collections.emptyList();
            }

            long matchSize = Math.min(remainingSize, level.size);
            remainingSize -= matchSize;
            matches.add(new OrderMatch(levelPrice, matchSize));
        }

        return matches;
    }

    // --- Package-visible accessors for tests ---

    TreeMap<Long, OrderBookLevel> bids() {
        return bids;
    }

    TreeMap<Long, OrderBookLevel> asks() {
        return asks;
    }

    Map<Long, LocalOrder> localBidOrders() {
        return localBidOrders;
    }

    Map<Long, LocalOrder> localAskOrders() {
        return localAskOrders;
    }

    private void reconcileSide(TreeMap<Long, OrderBookLevel> book, Map<Long, Long> curr) {
        Set<Long> allPrices = new HashSet<>(book.keySet());
        allPrices.addAll(curr.keySet());

        for (long price : allPrices) {
            if (price == PRICE_NULL) {
                continue;
            }
            OrderBookLevel prevLevel = book.get(price);
            long prevSize = prevLevel != null ? prevLevel.size : 0;
            long newSize = curr.getOrDefault(price, 0L);

            if (newSize == 0) {
                if (prevLevel == null || !prevLevel.hasLocalOrders()) {
                    book.remove(price);
                    continue;
                }
                // Keep the level alive since we have local orders there
            }

            if (prevLevel == null) {
                prevLevel = new OrderBookLevel(price, 0);
                book.put(price, prevLevel);
            }

            queueModel.onModify(prevSize, newSize, prevLevel.localOrders);
            prevLevel.size = newSize;
        }
    }

    private void clearFills(List<LocalOrderFill> fills) {
        for (LocalOrderFill fill : fills) {
            LocalOrder lo = fill.localOrder();
            if (lo.remaining == 0) {
                long clientOid = lo.order.getClientOidCounter();
                localBidOrders.remove(clientOid);
                localAskOrders.remove(clientOid);
            }
        }
    }
}
