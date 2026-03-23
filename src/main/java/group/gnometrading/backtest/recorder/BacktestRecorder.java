package group.gnometrading.backtest.recorder;

import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.schemas.*;

import java.util.Arrays;

public class BacktestRecorder {

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
        int i = marketCount++;
        marketTimestamp[i] = timestamp;

        switch (data.schemaType) {
            case MBP_10 -> writeMBP10(i, (MBP10Schema) data);
            case MBP_1 -> writeMBP1(i, (MBP1Schema) data);
            case BBO_1S -> writeBBO1S(i, (BBO1SSchema) data);
            case BBO_1M -> writeBBO1M(i, (BBO1MSchema) data);
            case TRADES -> writeTrades(i, (TradesSchema) data);
            case MBO -> writeMBO(i, (MBOSchema) data);
            case OHLCV_1S -> writeOHLCV1S(i, (OHLCV1SSchema) data);
            case OHLCV_1M -> writeOHLCV1M(i, (OHLCV1MSchema) data);
            case OHLCV_1H -> writeOHLCV1H(i, (OHLCV1HSchema) data);
        }
    }

    // --- Execution report recording ---

    public void onExecutionReport(long timestamp, BacktestExecutionReport report) {
        if (execCount == execTimestampEvent.length) {
            growExecArrays();
        }
        int i = execCount++;
        execTimestampEvent[i] = report.timestampEvent;
        execTimestampRecv[i] = report.timestampRecv;
        execExchangeId[i] = report.exchangeId;
        execSecurityId[i] = report.securityId;
        execClientOid[i] = report.clientOid;
        execSide[i] = report.side.name();
        execExecType[i] = report.execType.name();
        execOrderStatus[i] = report.orderStatus.name();
        execFilledQty[i] = report.filledQty;
        execFillPrice[i] = report.fillPrice;
        execCumulativeQty[i] = report.cumulativeQty;
        execLeavesQty[i] = report.leavesQty;
        execFee[i] = report.fee;
    }

    // --- Counts ---

    public int getMarketRecordCount() { return marketCount; }
    public int getExecutionRecordCount() { return execCount; }

    // --- Market array getters ---

    public long[] getMarketTimestamps() { return marketTimestamp; }
    public int[]  getMarketExchangeIds() { return marketExchangeId; }
    public long[] getMarketSecurityIds() { return marketSecurityId; }
    public long[] getMarketBestBidPrices() { return marketBestBidPrice; }
    public long[] getMarketBestAskPrices() { return marketBestAskPrice; }
    public long[] getMarketBestBidSizes() { return marketBestBidSize; }
    public long[] getMarketBestAskSizes() { return marketBestAskSize; }
    public long[] getMarketLastTradePrices() { return marketLastTradePrice; }
    public long[] getMarketLastTradeSizes() { return marketLastTradeSize; }
    public long[] getMarketSequenceNumbers() { return marketSequenceNumber; }

    // --- Execution array getters ---

    public long[]   getExecTimestampEvents() { return execTimestampEvent; }
    public long[]   getExecTimestampRecvs() { return execTimestampRecv; }
    public int[]    getExecExchangeIds() { return execExchangeId; }
    public int[]    getExecSecurityIds() { return execSecurityId; }
    public String[] getExecClientOids() { return execClientOid; }
    public String[] getExecSides() { return execSide; }
    public String[] getExecExecTypes() { return execExecType; }
    public String[] getExecOrderStatuses() { return execOrderStatus; }
    public long[]   getExecFilledQtys() { return execFilledQty; }
    public long[]   getExecFillPrices() { return execFillPrice; }
    public long[]   getExecCumulativeQtys() { return execCumulativeQty; }
    public long[]   getExecLeavesQtys() { return execLeavesQty; }
    public double[] getExecFees() { return execFee; }

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

    private void writeMBP10(int i, MBP10Schema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.bidPrice0();
        marketBestAskPrice[i] = d.askPrice0();
        marketBestBidSize[i] = d.bidSize0();
        marketBestAskSize[i] = d.askSize0();
        marketLastTradePrice[i] = d.price();
        marketLastTradeSize[i] = d.size();
        marketSequenceNumber[i] = d.sequence();
    }

    private void writeMBP1(int i, MBP1Schema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.bidPrice0();
        marketBestAskPrice[i] = d.askPrice0();
        marketBestBidSize[i] = d.bidSize0();
        marketBestAskSize[i] = d.askSize0();
        marketLastTradePrice[i] = d.price();
        marketLastTradeSize[i] = d.size();
        marketSequenceNumber[i] = d.sequence();
    }

    private void writeBBO1S(int i, BBO1SSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.bidPrice0();
        marketBestAskPrice[i] = d.askPrice0();
        marketBestBidSize[i] = d.bidSize0();
        marketBestAskSize[i] = d.askSize0();
        marketLastTradePrice[i] = d.price();
        marketLastTradeSize[i] = d.size();
        marketSequenceNumber[i] = d.sequence();
    }

    private void writeBBO1M(int i, BBO1MSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.bidPrice0();
        marketBestAskPrice[i] = d.askPrice0();
        marketBestBidSize[i] = d.bidSize0();
        marketBestAskSize[i] = d.askSize0();
        marketLastTradePrice[i] = d.price();
        marketLastTradeSize[i] = d.size();
        marketSequenceNumber[i] = d.sequence();
    }

    private void writeTrades(int i, TradesSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketLastTradePrice[i] = d.price();
        marketLastTradeSize[i] = d.size();
        marketSequenceNumber[i] = d.sequence();
    }

    private void writeMBO(int i, MBOSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketLastTradePrice[i] = d.price();
        marketLastTradeSize[i] = d.size();
        marketSequenceNumber[i] = d.sequence();
    }

    private void writeOHLCV1S(int i, OHLCV1SSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.close();
        marketBestAskPrice[i] = d.close();
        marketLastTradePrice[i] = d.close();
    }

    private void writeOHLCV1M(int i, OHLCV1MSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.close();
        marketBestAskPrice[i] = d.close();
        marketLastTradePrice[i] = d.close();
    }

    private void writeOHLCV1H(int i, OHLCV1HSchema s) {
        var d = s.decoder;
        marketExchangeId[i] = d.exchangeId();
        marketSecurityId[i] = d.securityId();
        marketBestBidPrice[i] = d.close();
        marketBestAskPrice[i] = d.close();
        marketLastTradePrice[i] = d.close();
    }
}
