package group.gnometrading.backtest.recorder;

import group.gnometrading.schemas.Bbo1mSchema;
import group.gnometrading.schemas.Bbo1sSchema;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.Ohlcv1hSchema;
import group.gnometrading.schemas.Ohlcv1mSchema;
import group.gnometrading.schemas.Ohlcv1sSchema;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.TradesSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Columnar in-memory recorder for backtest events.
 *
 * <p>All data is stored in {@link RecordBuffer} instances — one per event stream. The four
 * built-in streams (market, orders, fills, intents) are created and frozen at construction time.
 * Strategies may add custom streams via {@link #createBuffer} through a {@link MetricRecorder}
 * handle.
 *
 * <p>Built-in streams:
 * <ol>
 *   <li><b>market</b> — one row per market data tick: BBO + configurable depth, mid, spread,
 *       imbalance, last trade.
 *   <li><b>orders</b> — one row per order at terminal state (FILLED, CANCELLED, REJECTED, EXPIRED),
 *       with accumulated fill aggregates.
 *   <li><b>fills</b> — one row per fill event (PARTIAL_FILL or FILL), annotated with BBO at fill
 *       time for slippage analysis.
 *   <li><b>intents</b> — one row per strategy intent published to the OMS.
 * </ol>
 */
public final class BacktestRecorder {

    /** Maximum number of book depth levels that can be recorded (matches MBP_10). */
    public static final int MAX_DEPTH = 10;

    private static final int INITIAL_MARKET_CAPACITY = 1_000_000;
    private static final int INITIAL_ORDER_CAPACITY = 10_000;
    private static final int INITIAL_FILL_CAPACITY = 50_000;
    private static final int INITIAL_INTENT_CAPACITY = 1_000_000;

    private final int recordDepth;

    // =========================================================================
    // Built-in buffers
    // =========================================================================

    private final RecordBuffer marketRecords;
    private final RecordBuffer orderRecords;
    private final RecordBuffer fillRecords;
    private final RecordBuffer intentRecords;
    private final List<RecordBuffer> customBuffers = new ArrayList<>();

    // -------------------------------------------------------------------------
    // Market column indices
    // -------------------------------------------------------------------------

    private final int mktTimestamp;
    private final int mktExchangeId;
    private final int mktSecurityId;
    // per-level: index i corresponds to column mktBidPrices[i] .. mktAskSizes[i]
    private final int[] mktBidPrices;
    private final int[] mktBidSizes;
    private final int[] mktAskPrices;
    private final int[] mktAskSizes;
    private final int mktLastTradePrice;
    private final int mktLastTradeSize;

    // -------------------------------------------------------------------------
    // Order column indices
    // -------------------------------------------------------------------------

    private final int ordSubmitTs;
    private final int ordAckTs;
    private final int ordTerminalTs;
    private final int ordExchangeId;
    private final int ordSecurityId;
    private final int ordStrategyId;
    private final int ordClientOid;
    private final int ordSide;
    private final int ordOrderType;
    private final int ordSubmitPrice;
    private final int ordSubmitSize;
    private final int ordFilledQty;
    private final int ordLeavesQty;
    private final int ordTotalCost;
    private final int ordTotalFee;
    private final int ordFinalStatus;

    // -------------------------------------------------------------------------
    // Fill column indices
    // -------------------------------------------------------------------------

    private final int filTimestamp;
    private final int filExchangeId;
    private final int filSecurityId;
    private final int filStrategyId;
    private final int filClientOid;
    private final int filSide;
    private final int filPrice;
    private final int filQty;
    private final int filLeavesQty;
    private final int filFee;
    private final int filBookBidPrice;
    private final int filBookAskPrice;

    // -------------------------------------------------------------------------
    // Intent column indices
    // -------------------------------------------------------------------------

    private final int intTimestamp;
    private final int intExchangeId;
    private final int intSecurityId;
    private final int intStrategyId;
    private final int intBidPrice;
    private final int intBidSize;
    private final int intAskPrice;
    private final int intAskSize;
    private final int intTakeSide;
    private final int intTakeSize;
    private final int intTakeLimitPrice;

    // =========================================================================
    // BBO cache and in-flight order state
    // =========================================================================

    private final HashMap<Long, long[]> bboCache = new HashMap<>();

    private static final class InFlightOrder {
        long submitTimestamp;
        long ackTimestamp = 0;
        int exchangeId;
        int securityId;
        int strategyId;
        byte side;
        byte orderType;
        long submitPrice;
        long submitSize;
        long filledQty = 0;
        long totalCost = 0;
        double totalFee = 0.0;
    }

    private final HashMap<Long, InFlightOrder> inFlight = new HashMap<>();

    // =========================================================================
    // Byte-encoding constants
    // =========================================================================

    /** Side byte encoding: None. */
    public static final byte SIDE_NONE = 0;
    /** Side byte encoding: Bid. */
    public static final byte SIDE_BID = 1;
    /** Side byte encoding: Ask. */
    public static final byte SIDE_ASK = 2;

    /** Order-type byte encoding: Limit. */
    public static final byte ORDER_TYPE_LIMIT = 0;
    /** Order-type byte encoding: Market. */
    public static final byte ORDER_TYPE_MARKET = 1;

    /** Final-status byte encoding: Filled. */
    public static final byte STATUS_FILLED = 0;
    /** Final-status byte encoding: PartialFill. */
    public static final byte STATUS_PARTIAL = 1;
    /** Final-status byte encoding: Cancelled. */
    public static final byte STATUS_CANCELLED = 2;
    /** Final-status byte encoding: Rejected. */
    public static final byte STATUS_REJECTED = 3;
    /** Final-status byte encoding: Expired. */
    public static final byte STATUS_EXPIRED = 4;

    // =========================================================================
    // Constructors
    // =========================================================================

    /** Creates a recorder that captures BBO only (depth = 1). */
    public BacktestRecorder() {
        this(1);
    }

    /**
     * Creates a recorder that captures {@code recordDepth} book levels.
     *
     * @param recordDepth number of bid/ask levels to record (1 = BBO only, max {@value MAX_DEPTH})
     */
    public BacktestRecorder(int recordDepth) {
        if (recordDepth < 1 || recordDepth > MAX_DEPTH) {
            throw new IllegalArgumentException("recordDepth must be 1–" + MAX_DEPTH + ", got: " + recordDepth);
        }
        this.recordDepth = recordDepth;

        // --- market buffer ---
        RecordBuffer mkt = new RecordBuffer("market", INITIAL_MARKET_CAPACITY);
        mktTimestamp = mkt.addLongColumn("timestamp");
        mktExchangeId = mkt.addIntColumn("exchange_id");
        mktSecurityId = mkt.addLongColumn("security_id");
        mktBidPrices = new int[recordDepth];
        mktBidSizes = new int[recordDepth];
        mktAskPrices = new int[recordDepth];
        mktAskSizes = new int[recordDepth];
        for (int l = 0; l < recordDepth; l++) {
            mktBidPrices[l] = mkt.addLongColumn("bid_price_" + l);
            mktBidSizes[l] = mkt.addLongColumn("bid_size_" + l);
            mktAskPrices[l] = mkt.addLongColumn("ask_price_" + l);
            mktAskSizes[l] = mkt.addLongColumn("ask_size_" + l);
        }
        mktLastTradePrice = mkt.addLongColumn("last_trade_price");
        mktLastTradeSize = mkt.addLongColumn("last_trade_size");
        mkt.freeze();
        marketRecords = mkt;

        // --- order buffer ---
        RecordBuffer ord = new RecordBuffer("orders", INITIAL_ORDER_CAPACITY);
        ordSubmitTs = ord.addLongColumn("submit_timestamp");
        ordAckTs = ord.addLongColumn("ack_timestamp");
        ordTerminalTs = ord.addLongColumn("terminal_timestamp");
        ordExchangeId = ord.addIntColumn("exchange_id");
        ordSecurityId = ord.addIntColumn("security_id");
        ordStrategyId = ord.addIntColumn("strategy_id");
        ordClientOid = ord.addLongColumn("client_oid");
        ordSide = ord.addByteColumn("side");
        ordOrderType = ord.addByteColumn("order_type");
        ordSubmitPrice = ord.addLongColumn("submit_price");
        ordSubmitSize = ord.addLongColumn("submit_size");
        ordFilledQty = ord.addLongColumn("filled_qty");
        ordLeavesQty = ord.addLongColumn("leaves_qty");
        ordTotalCost = ord.addLongColumn("total_cost");
        ordTotalFee = ord.addDoubleColumn("total_fee");
        ordFinalStatus = ord.addByteColumn("final_status");
        ord.freeze();
        orderRecords = ord;

        // --- fill buffer ---
        RecordBuffer fil = new RecordBuffer("fills", INITIAL_FILL_CAPACITY);
        filTimestamp = fil.addLongColumn("timestamp");
        filExchangeId = fil.addIntColumn("exchange_id");
        filSecurityId = fil.addIntColumn("security_id");
        filStrategyId = fil.addIntColumn("strategy_id");
        filClientOid = fil.addLongColumn("client_oid");
        filSide = fil.addByteColumn("side");
        filPrice = fil.addLongColumn("fill_price");
        filQty = fil.addLongColumn("fill_qty");
        filLeavesQty = fil.addLongColumn("leaves_qty");
        filFee = fil.addDoubleColumn("fee");
        filBookBidPrice = fil.addLongColumn("book_bid_price");
        filBookAskPrice = fil.addLongColumn("book_ask_price");
        fil.freeze();
        fillRecords = fil;

        // --- intent buffer ---
        RecordBuffer intent = new RecordBuffer("intents", INITIAL_INTENT_CAPACITY);
        intTimestamp = intent.addLongColumn("timestamp");
        intExchangeId = intent.addIntColumn("exchange_id");
        intSecurityId = intent.addLongColumn("security_id");
        intStrategyId = intent.addIntColumn("strategy_id");
        intBidPrice = intent.addLongColumn("bid_price");
        intBidSize = intent.addLongColumn("bid_size");
        intAskPrice = intent.addLongColumn("ask_price");
        intAskSize = intent.addLongColumn("ask_size");
        intTakeSide = intent.addStringColumn("take_side");
        intTakeSize = intent.addLongColumn("take_size");
        intTakeLimitPrice = intent.addLongColumn("take_limit_price");
        intent.freeze();
        intentRecords = intent;
    }

    // =========================================================================
    // Public recording API
    // =========================================================================

    /**
     * Records a market data tick and refreshes the BBO cache.
     * Called once per LOCAL_MARKET_DATA event from {@code BacktestDriver}.
     */
    public void onMarketData(long timestamp, Schema data) {
        int idx = marketRecords.appendRow();
        marketRecords.setLong(idx, mktTimestamp, timestamp);

        switch (data.schemaType) {
            case MBP_10 -> writeMbp10(idx, (Mbp10Schema) data);
            case MBP_1 -> writeMbp1(idx, (Mbp1Schema) data);
            case BBO_1S -> writeBbo1S(idx, (Bbo1sSchema) data);
            case BBO_1M -> writeBbo1M(idx, (Bbo1mSchema) data);
            case TRADES -> writeTrades(idx, (TradesSchema) data);
            case MBO -> writeMbo(idx, (MboSchema) data);
            case OHLCV_1S -> writeOhlcv1S(idx, (Ohlcv1sSchema) data);
            case OHLCV_1M -> writeOhlcv1M(idx, (Ohlcv1mSchema) data);
            case OHLCV_1H -> writeOhlcv1H(idx, (Ohlcv1hSchema) data);
        }
    }

    /**
     * Opens an in-flight order record when an {@code Order} is scheduled for the exchange.
     * Called from {@code BacktestDriver.scheduleLocalMessages()}.
     */
    public void onOrderSubmitted(long timestamp, Order order) {
        long clientOid = order.getClientOidCounter();
        InFlightOrder ifo = new InFlightOrder();
        ifo.submitTimestamp = timestamp;
        ifo.exchangeId = order.decoder.exchangeId();
        ifo.securityId = (int) order.decoder.securityId();
        ifo.strategyId = order.getClientOidStrategyId();
        ifo.side = encodeSide(order.decoder.side());
        ifo.orderType = encodeOrderType(order.decoder.orderType().name());
        ifo.submitPrice = order.decoder.price();
        ifo.submitSize = order.decoder.size();
        inFlight.put(clientOid, ifo);
    }

    /**
     * Dispatches an execution report based on its exec type.
     * Called from {@code OmsBacktestAdapter.processExecutionReport()}.
     */
    public void onExecution(long timestamp, OrderExecutionReport report, int strategyId, Side side) {
        ExecType et = report.decoder.execType();
        long clientOid = report.getClientOidCounter();

        switch (et) {
            case NEW -> ackOrder(timestamp, clientOid);
            case PARTIAL_FILL -> {
                appendFill(timestamp, report, strategyId, side, clientOid);
                accumulateFill(clientOid, report);
            }
            case FILL -> {
                appendFill(timestamp, report, strategyId, side, clientOid);
                accumulateFill(clientOid, report);
                finalizeOrder(timestamp, clientOid, report.decoder.leavesQty(), STATUS_FILLED);
            }
            case CANCEL -> finalizeOrder(timestamp, clientOid, report.decoder.leavesQty(), STATUS_CANCELLED);
            case REJECT -> finalizeOrder(timestamp, clientOid, 0, STATUS_REJECTED);
            case CANCEL_REJECT -> finalizeOrder(timestamp, clientOid, 0, STATUS_REJECTED);
            case EXPIRE -> finalizeOrder(timestamp, clientOid, report.decoder.leavesQty(), STATUS_EXPIRED);
            default -> {
                /* NULL_VAL — ignore */
            }
        }
    }

    /**
     * Records a strategy intent.
     * Called from {@code OmsBacktestAdapter.processIntents()}.
     */
    public void onIntent(long timestamp, Intent intent) {
        int idx = intentRecords.appendRow();
        intentRecords.setLong(idx, intTimestamp, timestamp);
        intentRecords.setInt(idx, intExchangeId, intent.decoder.exchangeId());
        intentRecords.setLong(idx, intSecurityId, intent.decoder.securityId());
        intentRecords.setInt(idx, intStrategyId, intent.decoder.strategyId());

        long bidPrice = intent.decoder.bidPrice();
        intentRecords.setLong(idx, intBidPrice, (bidPrice == IntentDecoder.bidPriceNullValue()) ? 0 : bidPrice);
        long bidSize = intent.decoder.bidSize();
        intentRecords.setLong(idx, intBidSize, (bidSize == IntentDecoder.bidSizeNullValue()) ? 0 : bidSize);
        long askPrice = intent.decoder.askPrice();
        intentRecords.setLong(idx, intAskPrice, (askPrice == IntentDecoder.askPriceNullValue()) ? 0 : askPrice);
        long askSize = intent.decoder.askSize();
        intentRecords.setLong(idx, intAskSize, (askSize == IntentDecoder.askSizeNullValue()) ? 0 : askSize);

        long takeSize = intent.decoder.takeSize();
        boolean hasTake = (takeSize != IntentDecoder.takeSizeNullValue()) && takeSize > 0;
        intentRecords.setString(
                idx, intTakeSide, hasTake ? intent.decoder.takeSide().name() : null);
        intentRecords.setLong(idx, intTakeSize, hasTake ? takeSize : 0);
        long takeLimitPrice = intent.decoder.takeLimitPrice();
        intentRecords.setLong(
                idx,
                intTakeLimitPrice,
                (takeLimitPrice == IntentDecoder.takeLimitPriceNullValue()) ? 0 : takeLimitPrice);
    }

    // =========================================================================
    // Custom buffer creation
    // =========================================================================

    /**
     * Creates a new named {@link RecordBuffer} for custom strategy metrics.
     *
     * <p>Called indirectly by strategies via {@link MetricRecorder}. The buffer is registered
     * in the custom list so {@link #getCustomBuffers()} exposes it to the Python bridge.
     *
     * <p>Callers must add columns and call {@link RecordBuffer#freeze()} before replay starts.
     */
    public RecordBuffer createBuffer(String name, int initialCapacity) {
        RecordBuffer buf = new RecordBuffer(name, initialCapacity);
        customBuffers.add(buf);
        return buf;
    }

    /**
     * Creates a {@link MetricRecorder} handle suitable for passing to a strategy.
     */
    public MetricRecorder createMetricRecorder() {
        return new MetricRecorder(this);
    }

    // =========================================================================
    // Buffer accessors for Python bridge
    // =========================================================================

    public RecordBuffer getMarketRecords() {
        return marketRecords;
    }

    public RecordBuffer getOrderRecords() {
        return orderRecords;
    }

    public RecordBuffer getFillRecords() {
        return fillRecords;
    }

    public RecordBuffer getIntentRecords() {
        return intentRecords;
    }

    public List<RecordBuffer> getCustomBuffers() {
        return Collections.unmodifiableList(customBuffers);
    }

    public int getRecordDepth() {
        return recordDepth;
    }

    public int getMarketRecordCount() {
        return marketRecords.getCount();
    }

    public int getOrderRecordCount() {
        return orderRecords.getCount();
    }

    public int getFillRecordCount() {
        return fillRecords.getCount();
    }

    public int getIntentRecordCount() {
        return intentRecords.getCount();
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    private void ackOrder(long timestamp, long clientOid) {
        InFlightOrder ifo = inFlight.get(clientOid);
        if (ifo != null && ifo.ackTimestamp == 0) {
            ifo.ackTimestamp = timestamp;
        }
    }

    private void appendFill(long timestamp, OrderExecutionReport report, int strategyId, Side side, long clientOid) {
        int idx = fillRecords.appendRow();
        int exchangeId = report.decoder.exchangeId();
        long securityId = report.decoder.securityId();

        fillRecords.setLong(idx, filTimestamp, timestamp);
        fillRecords.setInt(idx, filExchangeId, exchangeId);
        fillRecords.setInt(idx, filSecurityId, (int) securityId);
        fillRecords.setInt(idx, filStrategyId, strategyId);
        fillRecords.setLong(idx, filClientOid, clientOid);
        fillRecords.setByte(idx, filSide, encodeSide(side));
        fillRecords.setLong(idx, filPrice, report.decoder.fillPrice());
        fillRecords.setLong(idx, filQty, report.decoder.filledQty());
        fillRecords.setLong(idx, filLeavesQty, report.decoder.leavesQty());
        fillRecords.setDouble(idx, filFee, report.decoder.fee() / (double) Statics.PRICE_SCALING_FACTOR);

        long bboKey = bboKey(exchangeId, securityId);
        long[] bbo = bboCache.get(bboKey);
        if (bbo != null) {
            fillRecords.setLong(idx, filBookBidPrice, bbo[0]);
            fillRecords.setLong(idx, filBookAskPrice, bbo[1]);
        }
    }

    private void accumulateFill(long clientOid, OrderExecutionReport report) {
        InFlightOrder ifo = inFlight.get(clientOid);
        if (ifo == null) {
            return;
        }
        long qty = report.decoder.filledQty();
        long price = report.decoder.fillPrice();
        ifo.filledQty += qty;
        ifo.totalCost += price * qty;
        ifo.totalFee += report.decoder.fee() / (double) Statics.PRICE_SCALING_FACTOR;
    }

    private void finalizeOrder(long timestamp, long clientOid, long leavesQty, byte status) {
        InFlightOrder ifo = inFlight.remove(clientOid);
        if (ifo == null) {
            return;
        }

        int idx = orderRecords.appendRow();
        orderRecords.setLong(idx, ordSubmitTs, ifo.submitTimestamp);
        orderRecords.setLong(idx, ordAckTs, ifo.ackTimestamp);
        orderRecords.setLong(idx, ordTerminalTs, timestamp);
        orderRecords.setInt(idx, ordExchangeId, ifo.exchangeId);
        orderRecords.setInt(idx, ordSecurityId, ifo.securityId);
        orderRecords.setInt(idx, ordStrategyId, ifo.strategyId);
        orderRecords.setLong(idx, ordClientOid, clientOid);
        orderRecords.setByte(idx, ordSide, ifo.side);
        orderRecords.setByte(idx, ordOrderType, ifo.orderType);
        orderRecords.setLong(idx, ordSubmitPrice, ifo.submitPrice);
        orderRecords.setLong(idx, ordSubmitSize, ifo.submitSize);
        orderRecords.setLong(idx, ordFilledQty, ifo.filledQty);
        orderRecords.setLong(idx, ordLeavesQty, leavesQty);
        orderRecords.setLong(idx, ordTotalCost, ifo.totalCost);
        orderRecords.setDouble(idx, ordTotalFee, ifo.totalFee);
        orderRecords.setByte(idx, ordFinalStatus, status);
    }

    // =========================================================================
    // Market data extraction per schema type
    // =========================================================================

    private void writeBboCore(
            int idx,
            int exchangeId,
            long securityId,
            long bidPrice,
            long askPrice,
            long bidSize,
            long askSize,
            long tradePrice,
            long tradeSize) {
        marketRecords.setInt(idx, mktExchangeId, exchangeId);
        marketRecords.setLong(idx, mktSecurityId, securityId);
        marketRecords.setLong(idx, mktBidPrices[0], bidPrice);
        marketRecords.setLong(idx, mktAskPrices[0], askPrice);
        marketRecords.setLong(idx, mktBidSizes[0], bidSize);
        marketRecords.setLong(idx, mktAskSizes[0], askSize);
        marketRecords.setLong(idx, mktLastTradePrice, tradePrice);
        marketRecords.setLong(idx, mktLastTradeSize, tradeSize);
        updateBboCache(exchangeId, securityId, bidPrice, askPrice);
    }

    @SuppressWarnings({
        "checkstyle:CyclomaticComplexity",
        "checkstyle:MultipleVariableDeclarations",
        "checkstyle:NeedBraces"
    })
    private void writeMbp10(int idx, Mbp10Schema schema) {
        var dec = schema.decoder;
        int eid = dec.exchangeId();
        long sid = dec.securityId();
        writeBboCore(
                idx,
                eid,
                sid,
                dec.bidPrice0(),
                dec.askPrice0(),
                dec.bidSize0(),
                dec.askSize0(),
                dec.price(),
                dec.size());

        int depth = recordDepth;
        // Unrolled per-level writes — avoids reflection or virtual dispatch on the hot path
        if (depth >= 2) {
            marketRecords.setLong(idx, mktBidPrices[1], dec.bidPrice1());
            marketRecords.setLong(idx, mktAskPrices[1], dec.askPrice1());
            marketRecords.setLong(idx, mktBidSizes[1], dec.bidSize1());
            marketRecords.setLong(idx, mktAskSizes[1], dec.askSize1());
        }
        if (depth >= 3) {
            marketRecords.setLong(idx, mktBidPrices[2], dec.bidPrice2());
            marketRecords.setLong(idx, mktAskPrices[2], dec.askPrice2());
            marketRecords.setLong(idx, mktBidSizes[2], dec.bidSize2());
            marketRecords.setLong(idx, mktAskSizes[2], dec.askSize2());
        }
        if (depth >= 4) {
            marketRecords.setLong(idx, mktBidPrices[3], dec.bidPrice3());
            marketRecords.setLong(idx, mktAskPrices[3], dec.askPrice3());
            marketRecords.setLong(idx, mktBidSizes[3], dec.bidSize3());
            marketRecords.setLong(idx, mktAskSizes[3], dec.askSize3());
        }
        if (depth >= 5) {
            marketRecords.setLong(idx, mktBidPrices[4], dec.bidPrice4());
            marketRecords.setLong(idx, mktAskPrices[4], dec.askPrice4());
            marketRecords.setLong(idx, mktBidSizes[4], dec.bidSize4());
            marketRecords.setLong(idx, mktAskSizes[4], dec.askSize4());
        }
        if (depth >= 6) {
            marketRecords.setLong(idx, mktBidPrices[5], dec.bidPrice5());
            marketRecords.setLong(idx, mktAskPrices[5], dec.askPrice5());
            marketRecords.setLong(idx, mktBidSizes[5], dec.bidSize5());
            marketRecords.setLong(idx, mktAskSizes[5], dec.askSize5());
        }
        if (depth >= 7) {
            marketRecords.setLong(idx, mktBidPrices[6], dec.bidPrice6());
            marketRecords.setLong(idx, mktAskPrices[6], dec.askPrice6());
            marketRecords.setLong(idx, mktBidSizes[6], dec.bidSize6());
            marketRecords.setLong(idx, mktAskSizes[6], dec.askSize6());
        }
        if (depth >= 8) {
            marketRecords.setLong(idx, mktBidPrices[7], dec.bidPrice7());
            marketRecords.setLong(idx, mktAskPrices[7], dec.askPrice7());
            marketRecords.setLong(idx, mktBidSizes[7], dec.bidSize7());
            marketRecords.setLong(idx, mktAskSizes[7], dec.askSize7());
        }
        if (depth >= 9) {
            marketRecords.setLong(idx, mktBidPrices[8], dec.bidPrice8());
            marketRecords.setLong(idx, mktAskPrices[8], dec.askPrice8());
            marketRecords.setLong(idx, mktBidSizes[8], dec.bidSize8());
            marketRecords.setLong(idx, mktAskSizes[8], dec.askSize8());
        }
        if (depth >= 10) {
            marketRecords.setLong(idx, mktBidPrices[9], dec.bidPrice9());
            marketRecords.setLong(idx, mktAskPrices[9], dec.askPrice9());
            marketRecords.setLong(idx, mktBidSizes[9], dec.bidSize9());
            marketRecords.setLong(idx, mktAskSizes[9], dec.askSize9());
        }
    }

    private void writeMbp1(int idx, Mbp1Schema schema) {
        var dec = schema.decoder;
        writeBboCore(
                idx,
                dec.exchangeId(),
                dec.securityId(),
                dec.bidPrice0(),
                dec.askPrice0(),
                dec.bidSize0(),
                dec.askSize0(),
                dec.price(),
                dec.size());
    }

    private void writeBbo1S(int idx, Bbo1sSchema schema) {
        var dec = schema.decoder;
        writeBboCore(
                idx,
                dec.exchangeId(),
                dec.securityId(),
                dec.bidPrice0(),
                dec.askPrice0(),
                dec.bidSize0(),
                dec.askSize0(),
                dec.price(),
                dec.size());
    }

    private void writeBbo1M(int idx, Bbo1mSchema schema) {
        var dec = schema.decoder;
        writeBboCore(
                idx,
                dec.exchangeId(),
                dec.securityId(),
                dec.bidPrice0(),
                dec.askPrice0(),
                dec.bidSize0(),
                dec.askSize0(),
                dec.price(),
                dec.size());
    }

    private void writeTrades(int idx, TradesSchema schema) {
        var dec = schema.decoder;
        marketRecords.setInt(idx, mktExchangeId, dec.exchangeId());
        marketRecords.setLong(idx, mktSecurityId, dec.securityId());
        marketRecords.setLong(idx, mktLastTradePrice, dec.price());
        marketRecords.setLong(idx, mktLastTradeSize, dec.size());
    }

    private void writeMbo(int idx, MboSchema schema) {
        var dec = schema.decoder;
        marketRecords.setInt(idx, mktExchangeId, dec.exchangeId());
        marketRecords.setLong(idx, mktSecurityId, dec.securityId());
        marketRecords.setLong(idx, mktLastTradePrice, dec.price());
        marketRecords.setLong(idx, mktLastTradeSize, dec.size());
    }

    private void writeOhlcv1S(int idx, Ohlcv1sSchema schema) {
        var dec = schema.decoder;
        writeOhlcvClose(idx, dec.exchangeId(), dec.securityId(), dec.close());
    }

    private void writeOhlcv1M(int idx, Ohlcv1mSchema schema) {
        var dec = schema.decoder;
        writeOhlcvClose(idx, dec.exchangeId(), dec.securityId(), dec.close());
    }

    private void writeOhlcv1H(int idx, Ohlcv1hSchema schema) {
        var dec = schema.decoder;
        writeOhlcvClose(idx, dec.exchangeId(), dec.securityId(), dec.close());
    }

    private void writeOhlcvClose(int idx, int exchangeId, long securityId, long close) {
        marketRecords.setInt(idx, mktExchangeId, exchangeId);
        marketRecords.setLong(idx, mktSecurityId, securityId);
        marketRecords.setLong(idx, mktBidPrices[0], close);
        marketRecords.setLong(idx, mktAskPrices[0], close);
        marketRecords.setLong(idx, mktLastTradePrice, close);
        updateBboCache(exchangeId, securityId, close, close);
    }

    // =========================================================================
    // Utilities
    // =========================================================================

    private static long bboKey(int exchangeId, long securityId) {
        return ((long) exchangeId << 32) | (securityId & 0xFFFFFFFFL);
    }

    private void updateBboCache(int exchangeId, long securityId, long bidPrice, long askPrice) {
        long key = bboKey(exchangeId, securityId);
        long[] bbo = bboCache.computeIfAbsent(key, k -> new long[2]);
        bbo[0] = bidPrice;
        bbo[1] = askPrice;
    }

    private static byte encodeSide(Side side) {
        if (side == null || side == Side.None) {
            return SIDE_NONE;
        }
        return (side == Side.Bid) ? SIDE_BID : SIDE_ASK;
    }

    private static byte encodeOrderType(String name) {
        return "MARKET".equals(name) ? ORDER_TYPE_MARKET : ORDER_TYPE_LIMIT;
    }

    // =========================================================================
    // Clear
    // =========================================================================

    public void clear() {
        marketRecords.clear();
        orderRecords.clear();
        fillRecords.clear();
        intentRecords.clear();
        inFlight.clear();
        bboCache.clear();
    }
}
