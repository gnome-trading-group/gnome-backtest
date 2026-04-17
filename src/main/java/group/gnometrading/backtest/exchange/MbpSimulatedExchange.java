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
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.TimeInForce;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class MbpSimulatedExchange implements SimulatedExchange {

    private static final long PRICE_NULL = Mbp10Decoder.priceNullValue();
    private static final long SIZE_NULL = Mbp10Decoder.sizeNullValue();

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
    public List<OrderExecutionReport> onMarketData(Schema data) {
        if (data instanceof Mbp10Schema mbp10) {
            return onMbp10(mbp10);
        } else if (data instanceof Mbp1Schema mbp1) {
            return onMbp1(mbp1);
        }
        throw new IllegalArgumentException("Unsupported schema type: " + data.schemaType);
    }

    @Override
    public List<OrderExecutionReport> submitOrder(Order order) {
        long clientOid = order.getClientOidCounter();
        int strategyId = order.getClientOidStrategyId();

        // Auto-generate clientOid if not set (counter == 0 indicates unset)
        if (clientOid == 0) {
            clientOid = orderCounter.incrementAndGet();
            order.encodeClientOid(clientOid, strategyId);
        }

        long size = order.decoder.size();
        Side side = order.decoder.side();

        if (size <= 0 || side == null || side == Side.None) {
            return List.of(rejected(order));
        }
        if (order.decoder.orderType() == OrderType.LIMIT && order.decoder.price() <= 0) {
            return List.of(rejected(order));
        }

        if (order.decoder.orderType() == OrderType.MARKET) {
            return handleMarketOrder(order);
        } else if (order.decoder.orderType() == OrderType.LIMIT) {
            return handleLimitOrder(order);
        }
        throw new IllegalArgumentException("Unexpected order type: " + order.decoder.orderType());
    }

    @Override
    public List<OrderExecutionReport> cancelOrder(CancelOrder cancel) {
        long clientOid = cancel.getClientOidCounter();
        if (orderBook.cancelOrder(clientOid)) {
            return List.of(canceled(cancel));
        }
        return List.of(cancelRejected(cancel));
    }

    @Override
    public List<OrderExecutionReport> modifyOrder(ModifyOrder modify) {
        long clientOid = modify.getClientOidCounter();
        long newPrice = modify.decoder.price();
        long newSize = modify.decoder.size();
        if (orderBook.modifyLocalOrder(clientOid, newPrice, newSize)) {
            OrderExecutionReport report = makeReport(
                    modify.getClientOidCounter(),
                    modify.getClientOidStrategyId(),
                    ExecType.NEW,
                    OrderStatus.NEW,
                    0,
                    0,
                    0,
                    newSize,
                    0);
            report.encoder.exchangeId((short) modify.decoder.exchangeId()).securityId(modify.decoder.securityId());
            return List.of(report);
        }
        // FIX protocol sends CANCEL_REJECT when rejecting a modify/replace order
        return List.of(cancelRejected(modify));
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

    private List<OrderExecutionReport> onMbp10(Mbp10Schema schema) {
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

    private List<OrderExecutionReport> onMbp1(Mbp1Schema schema) {
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

    private List<OrderExecutionReport> mapFillsToReports(List<LocalOrderFill> fills) {
        List<OrderExecutionReport> reports = new ArrayList<>(fills.size());
        for (LocalOrderFill fill : fills) {
            reports.add(mapFillToReport(fill.localOrder(), fill.fillSize(), fill.remainingAfterFill()));
        }
        return reports;
    }

    private OrderExecutionReport mapFillToReport(LocalOrder localOrder, long filledQty, long remainingAfterFill) {
        long price = localOrder.order.decoder.price();
        // Use double to avoid overflow: price (~9e13) * size (~1e6) = ~9e19 exceeds Long.MAX_VALUE
        double notional = (double) filledQty * price;
        long cumulativeQty = localOrder.order.decoder.size() - remainingAfterFill;

        ExecType execType = remainingAfterFill == 0 ? ExecType.FILL : ExecType.PARTIAL_FILL;
        OrderStatus orderStatus = remainingAfterFill == 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;

        OrderExecutionReport report = makeReport(
                localOrder.order.getClientOidCounter(),
                localOrder.order.getClientOidStrategyId(),
                execType,
                orderStatus,
                filledQty,
                price,
                cumulativeQty,
                remainingAfterFill,
                toScaledFee(feeModel.calculateFee(notional, true)));
        report.encoder
                .exchangeId((short) localOrder.order.decoder.exchangeId())
                .securityId(localOrder.order.decoder.securityId());
        return report;
    }

    private List<OrderExecutionReport> handleMarketOrder(Order order) {
        List<OrderMatch> matches = orderBook.getMatchingOrders(order);
        if (matches.isEmpty()) {
            return List.of(rejected(order));
        }

        long totalFilled = 0;
        double totalNotional = 0;
        for (OrderMatch match : matches) {
            totalFilled += match.size();
            totalNotional += (double) match.price() * match.size();
        }

        long vwapPrice = totalFilled > 0 ? (long) (totalNotional / totalFilled) : 0;
        long orderSize = order.decoder.size();

        ExecType execType = totalFilled == orderSize ? ExecType.FILL : ExecType.PARTIAL_FILL;
        OrderStatus status = totalFilled == orderSize ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
        long remaining = orderSize - totalFilled;

        OrderExecutionReport report = makeReport(
                order.getClientOidCounter(),
                order.getClientOidStrategyId(),
                execType,
                status,
                totalFilled,
                vwapPrice,
                totalFilled,
                remaining,
                toScaledFee(feeModel.calculateFee(totalNotional, false)));
        report.encoder.exchangeId((short) order.decoder.exchangeId()).securityId(order.decoder.securityId());
        return List.of(report);
    }

    private List<OrderExecutionReport> handleLimitOrder(Order order) {
        List<OrderMatch> matches = orderBook.getMatchingOrders(order);
        List<OrderExecutionReport> reports = new ArrayList<>();

        long orderSize = order.decoder.size();
        int exchangeId = order.decoder.exchangeId();
        long securityId = order.decoder.securityId();
        long clientOid = order.getClientOidCounter();
        int strategyId = order.getClientOidStrategyId();

        if (!matches.isEmpty()) {
            long totalFilled = 0;
            double totalNotional = 0;
            for (OrderMatch match : matches) {
                totalFilled += match.size();
                totalNotional += (double) match.price() * match.size();
            }

            // Aggressive limit orders that immediately cross the spread are taking liquidity.
            long feeScaled = toScaledFee(feeModel.calculateFee(totalNotional, false));
            long vwapPrice = (long) (totalNotional / totalFilled);
            long remaining = orderSize - totalFilled;

            if (totalFilled == orderSize) {
                OrderExecutionReport report = makeReport(
                        clientOid,
                        strategyId,
                        ExecType.FILL,
                        OrderStatus.FILLED,
                        totalFilled,
                        vwapPrice,
                        totalFilled,
                        0,
                        feeScaled);
                report.encoder.exchangeId((short) exchangeId).securityId(securityId);
                return List.of(report);
            }

            OrderExecutionReport partialFill = makeReport(
                    clientOid,
                    strategyId,
                    ExecType.PARTIAL_FILL,
                    OrderStatus.PARTIALLY_FILLED,
                    totalFilled,
                    vwapPrice,
                    totalFilled,
                    remaining,
                    feeScaled);
            partialFill.encoder.exchangeId((short) exchangeId).securityId(securityId);

            if (order.decoder.timeInForce() == TimeInForce.FILL_OR_KILL) {
                return List.of(rejected(order));
            } else if (order.decoder.timeInForce() == TimeInForce.IMMEDIATE_OR_CANCELED) {
                OrderExecutionReport cancel = makeReport(
                        clientOid,
                        strategyId,
                        ExecType.CANCEL,
                        OrderStatus.PARTIALLY_FILLED,
                        0,
                        0,
                        totalFilled,
                        0,
                        feeScaled);
                cancel.encoder.exchangeId((short) exchangeId).securityId(securityId);
                return List.of(partialFill, cancel);
            } else {
                orderBook.addLocalOrder(order, remaining);
                OrderExecutionReport newAck =
                        makeReport(clientOid, strategyId, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, orderSize, 0);
                newAck.encoder.exchangeId((short) exchangeId).securityId(securityId);
                reports.add(newAck);
                reports.add(partialFill);
                return reports;
            }
        } else {
            // No immediate fill
            if (order.decoder.timeInForce() == TimeInForce.IMMEDIATE_OR_CANCELED
                    || order.decoder.timeInForce() == TimeInForce.FILL_OR_KILL) {
                return List.of(rejected(order));
            }
            orderBook.addLocalOrder(order);
            OrderExecutionReport newAck =
                    makeReport(clientOid, strategyId, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, orderSize, 0);
            newAck.encoder.exchangeId((short) exchangeId).securityId(securityId);
            return List.of(newAck);
        }
    }

    // --- Factory helpers ---

    private static long toScaledFee(double fee) {
        return (long) (fee * Statics.PRICE_SCALING_FACTOR);
    }

    private OrderExecutionReport makeReport(
            long clientOid,
            int strategyId,
            ExecType execType,
            OrderStatus orderStatus,
            long filledQty,
            long fillPrice,
            long cumulativeQty,
            long leavesQty,
            long feeScaled) {
        OrderExecutionReport report = new OrderExecutionReport();
        report.encodeClientOid(clientOid, strategyId);
        report.encoder
                .execType(execType)
                .orderStatus(orderStatus)
                .filledQty((int) filledQty)
                .fillPrice(fillPrice)
                .cumulativeQty((int) cumulativeQty)
                .leavesQty((int) leavesQty)
                .fee(feeScaled);
        return report;
    }

    private OrderExecutionReport rejected(Order order) {
        OrderExecutionReport report = makeReport(
                order.getClientOidCounter(),
                order.getClientOidStrategyId(),
                ExecType.REJECT,
                OrderStatus.REJECTED,
                0,
                0,
                0,
                0,
                0);
        report.encoder.exchangeId((short) order.decoder.exchangeId()).securityId(order.decoder.securityId());
        return report;
    }

    private OrderExecutionReport cancelRejected(CancelOrder cancel) {
        OrderExecutionReport report = makeReport(
                cancel.getClientOidCounter(),
                cancel.getClientOidStrategyId(),
                ExecType.CANCEL_REJECT,
                OrderStatus.NEW,
                0,
                0,
                0,
                0,
                0);
        report.encoder.exchangeId((short) cancel.decoder.exchangeId()).securityId(cancel.decoder.securityId());
        return report;
    }

    private OrderExecutionReport cancelRejected(ModifyOrder modify) {
        OrderExecutionReport report = makeReport(
                modify.getClientOidCounter(),
                modify.getClientOidStrategyId(),
                ExecType.CANCEL_REJECT,
                OrderStatus.NEW,
                0,
                0,
                0,
                0,
                0);
        report.encoder.exchangeId((short) modify.decoder.exchangeId()).securityId(modify.decoder.securityId());
        return report;
    }

    private OrderExecutionReport canceled(CancelOrder cancel) {
        OrderExecutionReport report = makeReport(
                cancel.getClientOidCounter(),
                cancel.getClientOidStrategyId(),
                ExecType.CANCEL,
                OrderStatus.CANCELED,
                0,
                0,
                0,
                0,
                0);
        report.encoder.exchangeId((short) cancel.decoder.exchangeId()).securityId(cancel.decoder.securityId());
        return report;
    }

    private List<BidAskLevel> extractMbp10Levels(Mbp10Schema schema) {
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

    private List<BidAskLevel> extractMbp1Levels(Mbp1Schema schema) {
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
