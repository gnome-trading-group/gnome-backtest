package group.gnometrading.backtest.book;

import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;

import java.util.*;

public class MBPBook {

    private static final long PRICE_NULL = Long.MIN_VALUE;

    private final QueueModel queueModel;

    // TreeMap bids: descending order (best bid first), asks: ascending (best ask first)
    private final TreeMap<Long, OrderBookLevel> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Long, OrderBookLevel> asks = new TreeMap<>();

    // local_orders[side][clientOid] -> LocalOrder
    private final Map<String, LocalOrder> localBidOrders = new HashMap<>();
    private final Map<String, LocalOrder> localAskOrders = new HashMap<>();

    public MBPBook(QueueModel queueModel) {
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

        // Check local bid orders that can be filled against the ask side
        for (long bidPrice : bids.keySet()) {
            if (bidPrice < bestAsk) {
                break;
            }
            OrderBookLevel bidLevel = bids.get(bidPrice);
            if (bidLevel == null || !bidLevel.hasLocalOrders()) {
                continue;
            }
            long remainingToFill = bidLevel.localOrders.stream().mapToLong(lo -> lo.remaining).sum();
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
                long filledQty = fills.stream().mapToLong(LocalOrderFill::fillSize).sum();
                remainingToFill -= filledQty;
                askLevel.size -= filledQty;
            }
        }

        // Check local ask orders that can be filled against the bid side
        for (long askPrice : asks.keySet()) {
            if (askPrice > bestBid) {
                break;
            }
            OrderBookLevel askLevel = asks.get(askPrice);
            if (askLevel == null || !askLevel.hasLocalOrders()) {
                continue;
            }
            long remainingToFill = askLevel.localOrders.stream().mapToLong(lo -> lo.remaining).sum();
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
                long filledQty = fills.stream().mapToLong(LocalOrderFill::fillSize).sum();
                remainingToFill -= filledQty;
                bidLevel.size -= filledQty;
            }
        }

        clearFills(allFills);
        return allFills;
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
            if (tradeIsBid && levelPrice > price) {
                break;
            }
            if (!tradeIsBid && levelPrice < price) {
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
    public void addLocalOrder(BacktestOrder order, long remaining) {
        TreeMap<Long, OrderBookLevel> book = order.side() == Side.Bid ? bids : asks;
        Map<String, LocalOrder> localOrders = order.side() == Side.Bid ? localBidOrders : localAskOrders;

        if (localOrders.containsKey(order.clientOid())) {
            throw new IllegalArgumentException("Duplicate client OID: " + order.clientOid());
        }

        OrderBookLevel level = book.get(order.price());
        if (level == null) {
            level = new OrderBookLevel(order.price(), 0);
            book.put(order.price(), level);
        }

        LocalOrder localOrder = new LocalOrder(order, remaining, level.size);
        localOrders.put(order.clientOid(), localOrder);
        level.localOrders.addLast(localOrder);
    }

    public void addLocalOrder(BacktestOrder order) {
        addLocalOrder(order, order.size());
    }

    /**
     * Cancels a local order by clientOid. Returns true if the order was found and removed.
     */
    public boolean cancelOrder(String clientOid) {
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
        OrderBookLevel level = book.get(localOrder.order.price());
        if (level != null) {
            level.localOrders.remove(localOrder);
        }
        return true;
    }

    /**
     * Returns immediate matches for the order by walking the opposite side of the book.
     * Throws if self-filling would occur.
     */
    public List<OrderMatch> getMatchingOrders(BacktestOrder order) {
        List<OrderMatch> matches = new ArrayList<>();
        long remainingSize = order.size();
        boolean isBuy = order.side() == Side.Bid;
        TreeMap<Long, OrderBookLevel> oppBook = isBuy ? asks : bids;

        for (Map.Entry<Long, OrderBookLevel> entry : oppBook.entrySet()) {
            if (remainingSize == 0) {
                break;
            }
            long levelPrice = entry.getKey();
            if (order.orderType() == OrderType.LIMIT) {
                if (isBuy && levelPrice > order.price()) {
                    break;
                }
                if (!isBuy && levelPrice < order.price()) {
                    break;
                }
            }

            OrderBookLevel level = entry.getValue();
            if (level.hasLocalOrders()) {
                throw new IllegalStateException("Self filling triggered — price level " + levelPrice + " has local orders");
            }
            if (level.size == 0) {
                throw new IllegalStateException("Malformed local book — price level has no size: " + levelPrice);
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

    Map<String, LocalOrder> localBidOrders() {
        return localBidOrders;
    }

    Map<String, LocalOrder> localAskOrders() {
        return localAskOrders;
    }

    private void reconcileSide(TreeMap<Long, OrderBookLevel> book, Map<Long, Long> curr) {
        Set<Long> allPrices = new HashSet<>(book.keySet());
        allPrices.addAll(curr.keySet());

        for (long price : allPrices) {
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
                localBidOrders.remove(lo.order.clientOid());
                localAskOrders.remove(lo.order.clientOid());
            }
        }
    }
}
