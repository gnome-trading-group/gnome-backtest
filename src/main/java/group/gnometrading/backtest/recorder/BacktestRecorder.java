package group.gnometrading.backtest.recorder;

import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.schemas.BBO1MSchema;
import group.gnometrading.schemas.BBO1SSchema;
import group.gnometrading.schemas.MBOSchema;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.MBP1Schema;
import group.gnometrading.schemas.OHLCV1HSchema;
import group.gnometrading.schemas.OHLCV1MSchema;
import group.gnometrading.schemas.OHLCV1SSchema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.TradesSchema;
import java.util.Arrays;

public final class BacktestRecorder {

    private static final int INITIAL_MARKET_CAPACITY = 1_000_000;
    private static final int INITIAL_EXEC_CAPACITY = 10_000;

    // Market record columnar arrays
    private int marketCount = 0;
    private long[] marketTimestamp;
    private int[]  marketExchangeId;
    private long[] marketSecurityId;
    private long[] marketBestBidPrice;
    private long[] marketBestAskPrice;
    private long[] marketBestBidSize;
    private long[] marketBestAskSize;
    private long[] marketLastTradePrice;
    private long[] marketLastTradeSize;
    private long[] marketSequenceNumber;

    // Execution record columnar arrays
    private int execCount = 0;
    private long[]   execTimestampEvent;
    private long[]   execTimestampRecv;
    private int[]    execExchangeId;
    private int[]    execSecurityId;
    private String[] execClientOid;
    private String[] execSide;
    private String[] execExecType;
    private String[] execOrderStatus;
    private long[]   execFilledQty;
    private long[]   execFillPrice;
    private long[]   execCumulativeQty;
    private long[]   execLeavesQty;
    private double[] execFee;

    public BacktestRecorder() {
        allocateMarketArrays(INITIAL_MARKET_CAPACITY);
        allocateExecArrays(INITIAL_EXEC_CAPACITY);
    }

    // --- Market data recording ---

    public void onMarketData(long timestamp, Schema data) {
        if (marketCount == marketTimestamp.length) {
            growMarketArrays();
        }
        int idx = marketCount++;
        marketTimestamp[idx] = timestamp;

        switch (data.schemaType) {
            case MBP_10 -> writeMbp10(idx, (MBP10Schema) data);
            case MBP_1 -> writeMbp1(idx, (MBP1Schema) data);
            case BBO_1S -> writeBbo1S(idx, (BBO1SSchema) data);
            case BBO_1M -> writeBbo1M(idx, (BBO1MSchema) data);
            case TRADES -> writeTrades(idx, (TradesSchema) data);
            case MBO -> writeMbo(idx, (MBOSchema) data);
            case OHLCV_1S -> writeOhlcv1S(idx, (OHLCV1SSchema) data);
            case OHLCV_1M -> writeOhlcv1M(idx, (OHLCV1MSchema) data);
            case OHLCV_1H -> writeOhlcv1H(idx, (OHLCV1HSchema) data);
        }
    }

    // --- Execution report recording ---

    public void onExecutionReport(long timestamp, BacktestExecutionReport report) {
        if (execCount == execTimestampEvent.length) {
            growExecArrays();
        }
        int idx = execCount++;
        execTimestampEvent[idx] = report.timestampEvent;
        execTimestampRecv[idx] = report.timestampRecv;
        execExchangeId[idx] = report.exchangeId;
        execSecurityId[idx] = report.securityId;
        execClientOid[idx] = report.clientOid;
        execSide[idx] = report.side.name();
        execExecType[idx] = report.execType.name();
        execOrderStatus[idx] = report.orderStatus.name();
        execFilledQty[idx] = report.filledQty;
        execFillPrice[idx] = report.fillPrice;
        execCumulativeQty[idx] = report.cumulativeQty;
        execLeavesQty[idx] = report.leavesQty;
        execFee[idx] = report.fee;
    }

    // --- Counts ---

    public int getMarketRecordCount() {
        return marketCount;
    }

    public int getExecutionRecordCount() {
        return execCount;
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

    public long[] getMarketLastTradePrices() {
        return marketLastTradePrice;
    }

    public long[] getMarketLastTradeSizes() {
        return marketLastTradeSize;
    }

    public long[] getMarketSequenceNumbers() {
        return marketSequenceNumber;
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

    public String[] getExecClientOids() {
        return execClientOid;
    }

    public String[] getExecSides() {
        return execSide;
    }

    public String[] getExecExecTypes() {
        return execExecType;
    }

    public String[] getExecOrderStatuses() {
        return execOrderStatus;
    }

    public long[] getExecFilledQtys() {
        return execFilledQty;
    }

    public long[] getExecFillPrices() {
        return execFillPrice;
    }

    public long[] getExecCumulativeQtys() {
        return execCumulativeQty;
    }

    public long[] getExecLeavesQtys() {
        return execLeavesQty;
    }

    public double[] getExecFees() {
        return execFee;
    }

    // --- Clear ---

    public void clear() {
        marketCount = 0;
        execCount = 0;
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
        marketLastTradePrice = new long[capacity];
        marketLastTradeSize = new long[capacity];
        marketSequenceNumber = new long[capacity];
    }

    private void allocateExecArrays(int capacity) {
        execTimestampEvent = new long[capacity];
        execTimestampRecv = new long[capacity];
        execExchangeId = new int[capacity];
        execSecurityId = new int[capacity];
        execClientOid = new String[capacity];
        execSide = new String[capacity];
        execExecType = new String[capacity];
        execOrderStatus = new String[capacity];
        execFilledQty = new long[capacity];
        execFillPrice = new long[capacity];
        execCumulativeQty = new long[capacity];
        execLeavesQty = new long[capacity];
        execFee = new double[capacity];
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
        marketLastTradePrice = Arrays.copyOf(marketLastTradePrice, newCap);
        marketLastTradeSize = Arrays.copyOf(marketLastTradeSize, newCap);
        marketSequenceNumber = Arrays.copyOf(marketSequenceNumber, newCap);
    }

    private void growExecArrays() {
        int newCap = execTimestampEvent.length * 2;
        execTimestampEvent = Arrays.copyOf(execTimestampEvent, newCap);
        execTimestampRecv = Arrays.copyOf(execTimestampRecv, newCap);
        execExchangeId = Arrays.copyOf(execExchangeId, newCap);
        execSecurityId = Arrays.copyOf(execSecurityId, newCap);
        execClientOid = Arrays.copyOf(execClientOid, newCap);
        execSide = Arrays.copyOf(execSide, newCap);
        execExecType = Arrays.copyOf(execExecType, newCap);
        execOrderStatus = Arrays.copyOf(execOrderStatus, newCap);
        execFilledQty = Arrays.copyOf(execFilledQty, newCap);
        execFillPrice = Arrays.copyOf(execFillPrice, newCap);
        execCumulativeQty = Arrays.copyOf(execCumulativeQty, newCap);
        execLeavesQty = Arrays.copyOf(execLeavesQty, newCap);
        execFee = Arrays.copyOf(execFee, newCap);
    }

    // --- Schema-specific field extraction (inline, no object creation) ---

    private void writeMbp10(int idx, MBP10Schema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.bidPrice0();
        marketBestAskPrice[idx] = decoder.askPrice0();
        marketBestBidSize[idx] = decoder.bidSize0();
        marketBestAskSize[idx] = decoder.askSize0();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
        marketSequenceNumber[idx] = decoder.sequence();
    }

    private void writeMbp1(int idx, MBP1Schema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.bidPrice0();
        marketBestAskPrice[idx] = decoder.askPrice0();
        marketBestBidSize[idx] = decoder.bidSize0();
        marketBestAskSize[idx] = decoder.askSize0();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
        marketSequenceNumber[idx] = decoder.sequence();
    }

    private void writeBbo1S(int idx, BBO1SSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.bidPrice0();
        marketBestAskPrice[idx] = decoder.askPrice0();
        marketBestBidSize[idx] = decoder.bidSize0();
        marketBestAskSize[idx] = decoder.askSize0();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
        marketSequenceNumber[idx] = decoder.sequence();
    }

    private void writeBbo1M(int idx, BBO1MSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.bidPrice0();
        marketBestAskPrice[idx] = decoder.askPrice0();
        marketBestBidSize[idx] = decoder.bidSize0();
        marketBestAskSize[idx] = decoder.askSize0();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
        marketSequenceNumber[idx] = decoder.sequence();
    }

    private void writeTrades(int idx, TradesSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
        marketSequenceNumber[idx] = decoder.sequence();
    }

    private void writeMbo(int idx, MBOSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketLastTradePrice[idx] = decoder.price();
        marketLastTradeSize[idx] = decoder.size();
        marketSequenceNumber[idx] = decoder.sequence();
    }

    private void writeOhlcv1S(int idx, OHLCV1SSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.close();
        marketBestAskPrice[idx] = decoder.close();
        marketLastTradePrice[idx] = decoder.close();
    }

    private void writeOhlcv1M(int idx, OHLCV1MSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.close();
        marketBestAskPrice[idx] = decoder.close();
        marketLastTradePrice[idx] = decoder.close();
    }

    private void writeOhlcv1H(int idx, OHLCV1HSchema schema) {
        var decoder = schema.decoder;
        marketExchangeId[idx] = decoder.exchangeId();
        marketSecurityId[idx] = decoder.securityId();
        marketBestBidPrice[idx] = decoder.close();
        marketBestAskPrice[idx] = decoder.close();
        marketLastTradePrice[idx] = decoder.close();
    }
}
