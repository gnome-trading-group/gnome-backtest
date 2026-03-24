package group.gnometrading.backtest.exchange;

import group.gnometrading.backtest.book.BidAskLevel;
import group.gnometrading.backtest.book.LocalOrder;
import group.gnometrading.backtest.book.LocalOrderFill;
import group.gnometrading.backtest.book.MbpBook;
import group.gnometrading.backtest.book.OrderMatch;
import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.MBP1Schema;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.TimeInForce;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class MbpSimulatedExchange implements SimulatedExchange {

    private static final long PRICE_NULL = Long.MIN_VALUE;
    private static final long SIZE_NULL = 4294967295L;

    private final FeeModel feeModel;
    private final LatencyModel networkLatency;
    private final LatencyModel orderProcessingLatency;
    private final MbpBook orderBook;
    private final AtomicLong orderCounter = new AtomicLong(0);

    public MbpSimulatedExchange(
            FeeModel feeModel,
            LatencyModel networkLatency,
            LatencyModel orderProcessingLatency,
            QueueModel queueModel) {
        this.feeModel = feeModel;
        this.networkLatency = networkLatency;
        this.orderProcessingLatency = orderProcessingLatency;
        this.orderBook = new MbpBook(queueModel);
    }

    @Override
    public List<BacktestExecutionReport> onMarketData(Schema data) {
        if (data instanceof MBP10Schema mbp10) {
            return onMbp10(mbp10);
        } else if (data instanceof MBP1Schema mbp1) {
            return onMbp1(mbp1);
        }
        throw new IllegalArgumentException("Unsupported schema type: " + data.schemaType);
    }

    @Override
    public List<BacktestExecutionReport> submitOrder(BacktestOrder order) {
        BacktestOrder resolvedOrder = order.clientOid() == null
                ? new BacktestOrder(order.exchangeId(), order.securityId(), generateClientOid(),
                        order.side(), order.price(), order.size(), order.orderType(), order.timeInForce())
                : order;

        if (resolvedOrder.orderType() == OrderType.MARKET) {
            return handleMarketOrder(resolvedOrder);
        } else if (resolvedOrder.orderType() == OrderType.LIMIT) {
            return handleLimitOrder(resolvedOrder);
        }
        throw new IllegalArgumentException("Unexpected order type: " + resolvedOrder.orderType());
    }

    @Override
    public List<BacktestExecutionReport> cancelOrder(BacktestCancelOrder cancel) {
        if (orderBook.cancelOrder(cancel.clientOid())) {
            return List.of(BacktestExecutionReport.canceled(cancel.clientOid()));
        }
        return List.of(BacktestExecutionReport.rejected(cancel.clientOid()));
    }

    @Override
    public long simulateNetworkLatency() {
        return networkLatency.simulate();
    }

    @Override
    public long simulateOrderProcessingTime() {
        return orderProcessingLatency.simulate();
    }

    @Override
    public List<SchemaType> getSupportedSchemas() {
        return List.of(SchemaType.MBP_10, SchemaType.MBP_1);
    }

    private List<BacktestExecutionReport> onMbp10(MBP10Schema schema) {
        Action action = schema.decoder.action();
        if (action == Action.Add || action == Action.Cancel || action == Action.Modify) {
            List<BidAskLevel> levels = extractMbp10Levels(schema);
            List<LocalOrderFill> fills = orderBook.onMarketUpdate(levels);
            return mapFillsToReports(fills);
        } else if (action == Action.Trade) {
            long price = schema.decoder.price();
            long size = schema.decoder.size();
            var side = schema.decoder.side();
            List<LocalOrderFill> fills = orderBook.onTrade(price, size, side);
            return mapFillsToReports(fills);
        }
        return List.of();
    }

    private List<BacktestExecutionReport> onMbp1(MBP1Schema schema) {
        Action action = schema.decoder.action();
        if (action == Action.Add || action == Action.Cancel || action == Action.Modify) {
            List<BidAskLevel> levels = extractMbp1Levels(schema);
            List<LocalOrderFill> fills = orderBook.onMarketUpdate(levels);
            return mapFillsToReports(fills);
        } else if (action == Action.Trade) {
            long price = schema.decoder.price();
            long size = schema.decoder.size();
            var side = schema.decoder.side();
            List<LocalOrderFill> fills = orderBook.onTrade(price, size, side);
            return mapFillsToReports(fills);
        }
        return List.of();
    }

    private List<BacktestExecutionReport> mapFillsToReports(List<LocalOrderFill> fills) {
        List<BacktestExecutionReport> reports = new ArrayList<>(fills.size());
        for (LocalOrderFill fill : fills) {
            reports.add(mapFillToReport(fill.localOrder(), fill.fillSize()));
        }
        return reports;
    }

    private BacktestExecutionReport mapFillToReport(LocalOrder localOrder, long filledQty) {
        // Use double to avoid overflow: price (~9e13) * size (~1e6) = ~9e19 exceeds Long.MAX_VALUE
        double notional = (double) filledQty * localOrder.order.price();
        double fee = feeModel.calculateFee(notional, true);
        long fillPrice = localOrder.order.price();
        long cumulativeQty = localOrder.order.size() - localOrder.remaining;

        return new BacktestExecutionReport(
                localOrder.order.clientOid(),
                ExecType.FILL,
                localOrder.remaining == 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED,
                filledQty,
                fillPrice,
                cumulativeQty,
                localOrder.remaining,
                fee
        );
    }

    private List<BacktestExecutionReport> handleMarketOrder(BacktestOrder order) {
        List<OrderMatch> matches = orderBook.getMatchingOrders(order);
        if (matches.isEmpty()) {
            return List.of(BacktestExecutionReport.rejected(order.clientOid()));
        }

        long totalFilled = 0;
        double totalNotional = 0;
        for (OrderMatch match : matches) {
            totalFilled += match.size();
            totalNotional += (double) match.price() * match.size();
        }

        double fee = feeModel.calculateFee(totalNotional, false);
        long vwapPrice = totalFilled > 0 ? (long) (totalNotional / totalFilled) : 0;

        if (totalFilled == order.size()) {
            return List.of(new BacktestExecutionReport(
                    order.clientOid(), ExecType.FILL, OrderStatus.FILLED,
                    totalFilled, vwapPrice, totalFilled, 0, fee));
        } else {
            return List.of(new BacktestExecutionReport(
                    order.clientOid(), ExecType.PARTIAL_FILL, OrderStatus.PARTIALLY_FILLED,
                    totalFilled, vwapPrice,
                    totalFilled, order.size() - totalFilled, fee));
        }
    }

    private List<BacktestExecutionReport> handleLimitOrder(BacktestOrder order) {
        List<OrderMatch> matches = orderBook.getMatchingOrders(order);
        List<BacktestExecutionReport> reports = new ArrayList<>();

        if (!matches.isEmpty()) {
            long totalFilled = 0;
            double totalNotional = 0;
            for (OrderMatch match : matches) {
                totalFilled += match.size();
                totalNotional += (double) match.price() * match.size();
            }

            double fee = feeModel.calculateFee(totalNotional, true);
            long vwapPrice = (long) (totalNotional / totalFilled);

            if (totalFilled == order.size()) {
                return List.of(new BacktestExecutionReport(
                        order.clientOid(), ExecType.FILL, OrderStatus.FILLED,
                        totalFilled, vwapPrice, totalFilled, 0, fee));
            }

            // Partial immediate fill
            BacktestExecutionReport partialFill = new BacktestExecutionReport(
                    order.clientOid(), ExecType.PARTIAL_FILL, OrderStatus.PARTIALLY_FILLED,
                    totalFilled, vwapPrice, totalFilled, order.size() - totalFilled, fee);

            if (order.timeInForce() == TimeInForce.FILL_OR_KILL) {
                return List.of(BacktestExecutionReport.rejected(order.clientOid()));
            } else if (order.timeInForce() == TimeInForce.IMMEDIATE_OR_CANCELED) {
                return List.of(partialFill, new BacktestExecutionReport(
                        order.clientOid(), ExecType.CANCEL, OrderStatus.PARTIALLY_FILLED,
                        0, 0, totalFilled, 0, fee));
            } else {
                long remaining = order.size() - totalFilled;
                orderBook.addLocalOrder(order, remaining);
                reports.add(BacktestExecutionReport.newOrder(order.clientOid(), order.size()));
                reports.add(partialFill);
                return reports;
            }
        } else {
            // No immediate fill
            if (order.timeInForce() == TimeInForce.IMMEDIATE_OR_CANCELED
                    || order.timeInForce() == TimeInForce.FILL_OR_KILL) {
                return List.of(BacktestExecutionReport.rejected(order.clientOid()));
            }
            orderBook.addLocalOrder(order);
            return List.of(BacktestExecutionReport.newOrder(order.clientOid(), order.size()));
        }
    }

    private String generateClientOid() {
        return "client_" + orderCounter.incrementAndGet() + "_" + System.nanoTime();
    }

    private List<BidAskLevel> extractMbp10Levels(MBP10Schema schema) {
        var decoder = schema.decoder;
        List<BidAskLevel> levels = new ArrayList<>(10);
        addLevel(levels, decoder.bidPrice0(), decoder.bidSize0(), decoder.askPrice0(), decoder.askSize0());
        addLevel(levels, decoder.bidPrice1(), decoder.bidSize1(), decoder.askPrice1(), decoder.askSize1());
        addLevel(levels, decoder.bidPrice2(), decoder.bidSize2(), decoder.askPrice2(), decoder.askSize2());
        addLevel(levels, decoder.bidPrice3(), decoder.bidSize3(), decoder.askPrice3(), decoder.askSize3());
        addLevel(levels, decoder.bidPrice4(), decoder.bidSize4(), decoder.askPrice4(), decoder.askSize4());
        addLevel(levels, decoder.bidPrice5(), decoder.bidSize5(), decoder.askPrice5(), decoder.askSize5());
        addLevel(levels, decoder.bidPrice6(), decoder.bidSize6(), decoder.askPrice6(), decoder.askSize6());
        addLevel(levels, decoder.bidPrice7(), decoder.bidSize7(), decoder.askPrice7(), decoder.askSize7());
        addLevel(levels, decoder.bidPrice8(), decoder.bidSize8(), decoder.askPrice8(), decoder.askSize8());
        addLevel(levels, decoder.bidPrice9(), decoder.bidSize9(), decoder.askPrice9(), decoder.askSize9());
        return levels;
    }

    private List<BidAskLevel> extractMbp1Levels(MBP1Schema schema) {
        var decoder = schema.decoder;
        List<BidAskLevel> levels = new ArrayList<>(1);
        addLevel(levels, decoder.bidPrice0(), decoder.bidSize0(), decoder.askPrice0(), decoder.askSize0());
        return levels;
    }

    private void addLevel(List<BidAskLevel> levels, long bidPx, long bidSz, long askPx, long askSz) {
        if (bidPx != PRICE_NULL || askPx != PRICE_NULL) {
            levels.add(new BidAskLevel(
                    bidPx == PRICE_NULL ? PRICE_NULL : bidPx,
                    bidSz == SIZE_NULL ? 0 : bidSz,
                    askPx == PRICE_NULL ? PRICE_NULL : askPx,
                    askSz == SIZE_NULL ? 0 : askSz));
        }
    }
}
