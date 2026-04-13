package group.gnometrading.backtest.recorder;

import group.gnometrading.schemas.Bbo1mSchema;
import group.gnometrading.schemas.Bbo1sSchema;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.Ohlcv1hSchema;
import group.gnometrading.schemas.Ohlcv1mSchema;
import group.gnometrading.schemas.Ohlcv1sSchema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.TradesSchema;
import java.util.Arrays;

public final class BacktestRecorder {

    private static final int INITIAL_MARKET_CAPACITY = 1_000_000;
    private static final int INITIAL_EXEC_CAPACITY = 10_000;
    private static final int INITIAL_INTENT_CAPACITY = 1_000_000;

    // Market record columnar arrays
    private int marketCount = 0;
    private long[] marketTimestamp;
    private int[] marketExchangeId;
    private long[] marketSecurityId;
    private long[] marketBestBidPrice;
    private long[] marketBestAskPrice;
    private long[] marketBestBidSize;
    private long[] marketBestAskSize;
    private long[] marketMidPrice;
    private long[] marketSpread;
    private long[] marketLastTradePrice;
    private long[] marketLastTradeSize;

    // Execution record columnar arrays
    private int execCount = 0;
    private long[] execTimestampEvent;
    private long[] execTimestampRecv;
    private int[] execExchangeId;
    private int[] execSecurityId;
    private int[] execStrategyId;
    private String[] execClientOid;
    private String[] execSide;
    private String[] execExecType;
    private long[] execFilledQty;
    private long[] execFillPrice;
    private long[] execOrderPrice;
    private long[] execOrderSize;
    private double[] execFee;

    // Intent record columnar arrays
    private int intentCount = 0;
    private long[] intentTimestamp;
    private int[] intentExchangeId;
    private long[] intentSecurityId;
    private int[] intentStrategyId;
    private long[] intentBidPrice;
    private long[] intentBidSize;
    private long[] intentAskPrice;
    private long[] intentAskSize;
    private String[] intentTakeSide;
    private long[] intentTakeSize;
    private long[] intentTakeLimitPrice;

    public BacktestRecorder() {
        allocateMarketArrays(INITIAL_MARKET_CAPACITY);
        allocateExecArrays(INITIAL_EXEC_CAPACITY);
        allocateIntentArrays(INITIAL_INTENT_CAPACITY);
    }

    // --- Market data recording ---

    public void onMarketData(long timestamp, Schema data) {
        if (marketCount == marketTimestamp.length) {
            growMarketArrays();
        }
        int idx = marketCount++;
        marketTimestamp[idx] = timestamp;

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

    // --- Execution report recording ---

    public void onExecutionReport(
            long timestamp, OrderExecutionReport report, int strategyId, Side side, long orderPrice, long orderSize) {
        if (execCount == execTimestampEvent.length) {
            growExecArrays();
        }
        int idx = execCount++;
        execTimestampEvent[idx] = report.decoder.timestampEvent();
        execTimestampRecv[idx] = report.decoder.timestampRecv();
        execExchangeId[idx] = report.decoder.exchangeId();
        execSecurityId[idx] = (int) report.decoder.securityId();
        execStrategyId[idx] = strategyId;
        execClientOid[idx] = String.valueOf(report.getClientOidCounter());
        execSide[idx] = side != null ? side.name() : Side.None.name();
        execExecType[idx] = report.decoder.execType().name();
        execFilledQty[idx] = report.decoder.filledQty();
        execFillPrice[idx] = report.decoder.fillPrice();
        execOrderPrice[idx] = orderPrice;
        execOrderSize[idx] = orderSize;
        execFee[idx] = report.decoder.fee() / (double) Statics.PRICE_SCALING_FACTOR;
    }

    // --- Intent recording ---

    public void onIntent(long timestamp, Intent intent) {
        if (intentCount == intentTimestamp.length) {
            growIntentArrays();
        }
        int idx = intentCount++;
        intentTimestamp[idx] = timestamp;
        intentExchangeId[idx] = intent.decoder.exchangeId();
        intentSecurityId[idx] = intent.decoder.securityId();
        intentStrategyId[idx] = intent.decoder.strategyId();
        long bidPrice = intent.decoder.bidPrice();
        intentBidPrice[idx] = bidPrice == IntentDecoder.bidPriceNullValue() ? 0 : bidPrice;
        long bidSize = intent.decoder.bidSize();
        intentBidSize[idx] = bidSize == IntentDecoder.bidSizeNullValue() ? 0 : bidSize;
        long askPrice = intent.decoder.askPrice();
        intentAskPrice[idx] = askPrice == IntentDecoder.askPriceNullValue() ? 0 : askPrice;
        long askSize = intent.decoder.askSize();
        intentAskSize[idx] = askSize == IntentDecoder.askSizeNullValue() ? 0 : askSize;
        long takeSize = intent.decoder.takeSize();
        boolean hasTake = takeSize != IntentDecoder.takeSizeNullValue() && takeSize > 0;
        intentTakeSide[idx] = hasTake ? intent.decoder.takeSide().name() : null;
        intentTakeSize[idx] = hasTake ? takeSize : 0;
        long takeLimitPrice = intent.decoder.takeLimitPrice();
        intentTakeLimitPrice[idx] = takeLimitPrice == IntentDecoder.takeLimitPriceNullValue() ? 0 : takeLimitPrice;
    }

    // --- Counts ---

    public int getMarketRecordCount() {
        return marketCount;
    }

    public int getExecutionRecordCount() {
        return execCount;
    }

    public int getIntentRecordCount() {
        return intentCount;
    }

    // --- Market array getters ---

    public long[] getMarketTimestamps() {
        return marketTimestamp;
    }

    public int[] getMarketExchangeIds() {
        return marketExchangeId;
    }

    public long[] getMarketSecurityIds() {
        return marketSecurityId;
    }

    public long[] getMarketBestBidPrices() {
        return marketBestBidPrice;
    }

    public long[] getMarketBestAskPrices() {
        return marketBestAskPrice;
    }

    public long[] getMarketBestBidSizes() {
        return marketBestBidSize;
    }

    public long[] getMarketBestAskSizes() {
        return marketBestAskSize;
    }

    public long[] getMarketMidPrices() {
        return marketMidPrice;
    }

    public long[] getMarketSpreads() {
        return marketSpread;
    }

    public long[] getMarketLastTradePrices() {
        return marketLastTradePrice;
    }

    public long[] getMarketLastTradeSizes() {
        return marketLastTradeSize;
    }

    // --- Execution array getters ---

    public long[] getExecTimestampEvents() {
        return execTimestampEvent;
    }

    public long[] getExecTimestampRecvs() {
        return execTimestampRecv;
    }

    public int[] getExecExchangeIds() {
        return execExchangeId;
    }

    public int[] getExecSecurityIds() {
        return execSecurityId;
    }

    public int[] getExecStrategyIds() {
        return execStrategyId;
    }

    public String[] getExecClientOids() {
        return execClientOid;
    }

    public String[] getExecSides() {
        return execSide;
    }

    public String[] getExecExecTypes() {
        return execExecType;
    }

    public long[] getExecFilledQtys() {
        return execFilledQty;
    }

    public long[] getExecFillPrices() {
        return execFillPrice;
    }

    public long[] getExecOrderPrices() {
        return execOrderPrice;
    }

    public long[] getExecOrderSizes() {
        return execOrderSize;
    }

    public double[] getExecFees() {
        return execFee;
    }

    // --- Intent array getters ---

    public long[] getIntentTimestamps() {
        return intentTimestamp;
    }

    public int[] getIntentExchangeIds() {
        return intentExchangeId;
    }

    public long[] getIntentSecurityIds() {
        return intentSecurityId;
    }

    public int[] getIntentStrategyIds() {
        return intentStrategyId;
    }

    public long[] getIntentBidPrices() {
        return intentBidPrice;
    }

    public long[] getIntentBidSizes() {
        return intentBidSize;
    }

    public long[] getIntentAskPrices() {
        return intentAskPrice;
    }

    public long[] getIntentAskSizes() {
        return intentAskSize;
    }

    public String[] getIntentTakeSides() {
        return intentTakeSide;
    }

    public long[] getIntentTakeSizes() {
        return intentTakeSize;
    }

    public long[] getIntentTakeLimitPrices() {
        return intentTakeLimitPrice;
    }

    // --- Clear ---

    public void clear() {
        marketCount = 0;
        execCount = 0;
        intentCount = 0;
    }

    // --- Allocation and growth ---

    private void allocateMarketArrays(int capacity) {
        marketTimestamp = new long[capacity];
        marketExchangeId = new int[capacity];
        marketSecurityId = new long[capacity];
        marketBestBidPrice = new long[capacity];
        marketBestAskPrice = new long[capacity];
        marketBestBidSize = new long[capacity];
        marketBestAskSize = new long[capacity];
        marketMidPrice = new long[capacity];
        marketSpread = new long[capacity];
        marketLastTradePrice = new long[capacity];
        marketLastTradeSize = new long[capacity];
    }

    private void allocateExecArrays(int capacity) {
        execTimestampEvent = new long[capacity];
        execTimestampRecv = new long[capacity];
        execExchangeId = new int[capacity];
        execSecurityId = new int[capacity];
        execStrategyId = new int[capacity];
        execClientOid = new String[capacity];
        execSide = new String[capacity];
        execExecType = new String[capacity];
        execFilledQty = new long[capacity];
        execFillPrice = new long[capacity];
        execOrderPrice = new long[capacity];
        execOrderSize = new long[capacity];
        execFee = new double[capacity];
    }

    private void allocateIntentArrays(int capacity) {
        intentTimestamp = new long[capacity];
        intentExchangeId = new int[capacity];
        intentSecurityId = new long[capacity];
        intentStrategyId = new int[capacity];
        intentBidPrice = new long[capacity];
        intentBidSize = new long[capacity];
        intentAskPrice = new long[capacity];
        intentAskSize = new long[capacity];
        intentTakeSide = new String[capacity];
        intentTakeSize = new long[capacity];
        intentTakeLimitPrice = new long[capacity];
    }

    private void growMarketArrays() {
        int newCap = marketTimestamp.length * 2;
        marketTimestamp = Arrays.copyOf(marketTimestamp, newCap);
        marketExchangeId = Arrays.copyOf(marketExchangeId, newCap);
        marketSecurityId = Arrays.copyOf(marketSecurityId, newCap);
        marketBestBidPrice = Arrays.copyOf(marketBestBidPrice, newCap);
        marketBestAskPrice = Arrays.copyOf(marketBestAskPrice, newCap);
        marketBestBidSize = Arrays.copyOf(marketBestBidSize, newCap);
        marketBestAskSize = Arrays.copyOf(marketBestAskSize, newCap);
        marketMidPrice = Arrays.copyOf(marketMidPrice, newCap);
        marketSpread = Arrays.copyOf(marketSpread, newCap);
        marketLastTradePrice = Arrays.copyOf(marketLastTradePrice, newCap);
        marketLastTradeSize = Arrays.copyOf(marketLastTradeSize, newCap);
    }

    private void growExecArrays() {
        int newCap = execTimestampEvent.length * 2;
        execTimestampEvent = Arrays.copyOf(execTimestampEvent, newCap);
        execTimestampRecv = Arrays.copyOf(execTimestampRecv, newCap);
        execExchangeId = Arrays.copyOf(execExchangeId, newCap);
        execSecurityId = Arrays.copyOf(execSecurityId, newCap);
        execStrategyId = Arrays.copyOf(execStrategyId, newCap);
        execClientOid = Arrays.copyOf(execClientOid, newCap);
        execSide = Arrays.copyOf(execSide, newCap);
        execExecType = Arrays.copyOf(execExecType, newCap);
        execFilledQty = Arrays.copyOf(execFilledQty, newCap);
        execFillPrice = Arrays.copyOf(execFillPrice, newCap);
        execOrderPrice = Arrays.copyOf(execOrderPrice, newCap);
        execOrderSize = Arrays.copyOf(execOrderSize, newCap);
        execFee = Arrays.copyOf(execFee, newCap);
    }

    private void growIntentArrays() {
        int newCap = intentTimestamp.length * 2;
        intentTimestamp = Arrays.copyOf(intentTimestamp, newCap);
        intentExchangeId = Arrays.copyOf(intentExchangeId, newCap);
        intentSecurityId = Arrays.copyOf(intentSecurityId, newCap);
        intentStrategyId = Arrays.copyOf(intentStrategyId, newCap);
        intentBidPrice = Arrays.copyOf(intentBidPrice, newCap);
        intentBidSize = Arrays.copyOf(intentBidSize, newCap);
        intentAskPrice = Arrays.copyOf(intentAskPrice, newCap);
        intentAskSize = Arrays.copyOf(intentAskSize, newCap);
        intentTakeSide = Arrays.copyOf(intentTakeSide, newCap);
        intentTakeSize = Arrays.copyOf(intentTakeSize, newCap);
        intentTakeLimitPrice = Arrays.copyOf(intentTakeLimitPrice, newCap);
    }

    // --- Schema-specific field extraction ---

    private void writeBookFields(
            int idx,
            int exchangeId,
            long securityId,
            long bidPrice,
            long askPrice,
            long bidSize,
            long askSize,
            long tradePrice,
            long tradeSize) {
        marketExchangeId[idx] = exchangeId;
        marketSecurityId[idx] = securityId;
        marketBestBidPrice[idx] = bidPrice;
        marketBestAskPrice[idx] = askPrice;
        marketBestBidSize[idx] = bidSize;
        marketBestAskSize[idx] = askSize;
        marketMidPrice[idx] = (bidPrice > 0 && askPrice > 0) ? (bidPrice + askPrice) / 2 : 0;
        marketSpread[idx] = (bidPrice > 0 && askPrice > 0) ? askPrice - bidPrice : 0;
        marketLastTradePrice[idx] = tradePrice;
        marketLastTradeSize[idx] = tradeSize;
    }

    private void writeMbp10(int idx, Mbp10Schema schema) {
        var decoder = schema.decoder;
        writeBookFields(
                idx,
                decoder.exchangeId(),
                decoder.securityId(),
                decoder.bidPrice0(),
                decoder.askPrice0(),
                decoder.bidSize0(),
                decoder.askSize0(),
                decoder.price(),
                decoder.size());
    }

    private void writeMbp1(int idx, Mbp1Schema schema) {
        var decoder = schema.decoder;
        writeBookFields(
                idx,
                decoder.exchangeId(),
                decoder.securityId(),
                decoder.bidPrice0(),
                decoder.askPrice0(),
                decoder.bidSize0(),
                decoder.askSize0(),
                decoder.price(),
                decoder.size());
    }

    private void writeBbo1S(int idx, Bbo1sSchema schema) {
        var decoder = schema.decoder;
        writeBookFields(
                idx,
                decoder.exchangeId(),
                decoder.securityId(),
                decoder.bidPrice0(),
                decoder.askPrice0(),
                decoder.bidSize0(),
                decoder.askSize0(),
                decoder.price(),
                decoder.size());
    }

    private void writeBbo1M(int idx, Bbo1mSchema schema) {
        var decoder = schema.decoder;
        writeBookFields(
                idx,
                decoder.exchangeId(),
                decoder.securityId(),
                decoder.bidPrice0(),
                decoder.askPrice0(),
                decoder.bidSize0(),
                decoder.askSize0(),
                decoder.price(),
                decoder.size());
    }

    private void writeTrades(int idx, TradesSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
    }

    private void writeMbo(int idx, MboSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
    }

    private void writeOhlcv1S(int idx, Ohlcv1sSchema schema) {
        var decoder = schema.decoder;
        long close = decoder.close();
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = close;
        marketBestAskPrice[idx] = close;
        marketMidPrice[idx] = close;
        marketLastTradePrice[idx] = close;
    }

    private void writeOhlcv1M(int idx, Ohlcv1mSchema schema) {
        var decoder = schema.decoder;
        long close = decoder.close();
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = close;
        marketBestAskPrice[idx] = close;
        marketMidPrice[idx] = close;
        marketLastTradePrice[idx] = close;
    }

    private void writeOhlcv1H(int idx, Ohlcv1hSchema schema) {
        var decoder = schema.decoder;
        long close = decoder.close();
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = close;
        marketBestAskPrice[idx] = close;
        marketMidPrice[idx] = close;
        marketLastTradePrice[idx] = close;
    }
}
