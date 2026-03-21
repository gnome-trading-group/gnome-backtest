package group.gnometrading.backtest.exchange;

import group.gnometrading.backtest.book.BidAskLevel;
import group.gnometrading.backtest.book.LocalOrder;
import group.gnometrading.backtest.book.LocalOrderFill;
import group.gnometrading.backtest.book.MBPBook;
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

public class MBPSimulatedExchange implements SimulatedExchange {

    private static final long PRICE_NULL = Long.MIN_VALUE;
    private static final long SIZE_NULL = 4294967295L;

    private final FeeModel feeModel;
    private final LatencyModel networkLatency;
    private final LatencyModel orderProcessingLatency;
    private final MBPBook orderBook;
    private final AtomicLong orderCounter = new AtomicLong(0);

    public MBPSimulatedExchange(
            FeeModel feeModel,
            LatencyModel networkLatency,
            LatencyModel orderProcessingLatency,
            QueueModel queueModel) {
        this.feeModel = feeModel;
        this.networkLatency = networkLatency;
        this.orderProcessingLatency = orderProcessingLatency;
        this.orderBook = new MBPBook(queueModel);
    }

    @Override
    public List<BacktestExecutionReport> onMarketData(Schema data) {
        if (data instanceof MBP10Schema mbp10) {
            return onMBP10(mbp10);
        } else if (data instanceof MBP1Schema mbp1) {
            return onMBP1(mbp1);
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

    private List<BacktestExecutionReport> onMBP10(MBP10Schema schema) {
        Action action = schema.decoder.action();
        if (action == Action.Add || action == Action.Cancel || action == Action.Modify) {
            List<BidAskLevel> levels = extractMBP10Levels(schema);
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

    private List<BacktestExecutionReport> onMBP1(MBP1Schema schema) {
        Action action = schema.decoder.action();
        if (action == Action.Add || action == Action.Cancel || action == Action.Modify) {
            List<BidAskLevel> levels = extractMBP1Levels(schema);
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
        long totalPrice = filledQty * localOrder.order.price();
        double fee = feeModel.calculateFee(totalPrice, true);
        long fillPrice = filledQty > 0 ? totalPrice / filledQty : 0;
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
        long totalPrice = 0;
        for (OrderMatch match : matches) {
            totalFilled += match.size();
            totalPrice += match.price() * match.size();
        }

        double fee = feeModel.calculateFee(totalPrice, false);

        if (totalFilled == order.size()) {
            return List.of(new BacktestExecutionReport(
                    order.clientOid(), ExecType.FILL, OrderStatus.FILLED,
                    totalFilled, totalPrice / totalFilled, totalFilled, 0, fee));
        } else {
            // Partially filled market order
            return List.of(new BacktestExecutionReport(
                    order.clientOid(), ExecType.PARTIAL_FILL, OrderStatus.PARTIALLY_FILLED,
                    totalFilled, totalFilled > 0 ? totalPrice / totalFilled : 0,
                    totalFilled, order.size() - totalFilled, fee));
        }
    }

    private List<BacktestExecutionReport> handleLimitOrder(BacktestOrder order) {
        List<OrderMatch> matches = orderBook.getMatchingOrders(order);
        List<BacktestExecutionReport> reports = new ArrayList<>();

        if (!matches.isEmpty()) {
            long totalFilled = 0;
            long totalPrice = 0;
            for (OrderMatch match : matches) {
                totalFilled += match.size();
                totalPrice += match.price() * match.size();
            }

            double fee = feeModel.calculateFee(totalPrice, true);
            long fillPrice = totalPrice / totalFilled;

            if (totalFilled == order.size()) {
                return List.of(new BacktestExecutionReport(
                        order.clientOid(), ExecType.FILL, OrderStatus.FILLED,
                        totalFilled, fillPrice, totalFilled, 0, fee));
            }

            // Partial immediate fill
            BacktestExecutionReport partialFill = new BacktestExecutionReport(
                    order.clientOid(), ExecType.PARTIAL_FILL, OrderStatus.PARTIALLY_FILLED,
                    totalFilled, fillPrice, totalFilled, order.size() - totalFilled, fee);

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

    private List<BidAskLevel> extractMBP10Levels(MBP10Schema schema) {
        var d = schema.decoder;
        List<BidAskLevel> levels = new ArrayList<>(10);
        addLevel(levels, d.bidPrice0(), d.bidSize0(), d.askPrice0(), d.askSize0());
        addLevel(levels, d.bidPrice1(), d.bidSize1(), d.askPrice1(), d.askSize1());
        addLevel(levels, d.bidPrice2(), d.bidSize2(), d.askPrice2(), d.askSize2());
        addLevel(levels, d.bidPrice3(), d.bidSize3(), d.askPrice3(), d.askSize3());
        addLevel(levels, d.bidPrice4(), d.bidSize4(), d.askPrice4(), d.askSize4());
        addLevel(levels, d.bidPrice5(), d.bidSize5(), d.askPrice5(), d.askSize5());
        addLevel(levels, d.bidPrice6(), d.bidSize6(), d.askPrice6(), d.askSize6());
        addLevel(levels, d.bidPrice7(), d.bidSize7(), d.askPrice7(), d.askSize7());
        addLevel(levels, d.bidPrice8(), d.bidSize8(), d.askPrice8(), d.askSize8());
        addLevel(levels, d.bidPrice9(), d.bidSize9(), d.askPrice9(), d.askSize9());
        return levels;
    }

    private List<BidAskLevel> extractMBP1Levels(MBP1Schema schema) {
        var d = schema.decoder;
        List<BidAskLevel> levels = new ArrayList<>(1);
        addLevel(levels, d.bidPrice0(), d.bidSize0(), d.askPrice0(), d.askSize0());
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
