package group.gnometrading.backtest.recorder;

import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.schemas.*;

import java.util.ArrayList;
import java.util.List;

public class BacktestRecorder {

  private final List<MarketRecord> marketRecords = new ArrayList<>();
  private final List<ExecutionRecord> executionRecords = new ArrayList<>();

  public void onMarketData(long timestamp, Schema data) {
    MarketRecord record = switch (data.schemaType) {
      case MBP_10 -> fromMBP10(timestamp, (MBP10Schema) data);
      case MBP_1 -> fromMBP1(timestamp, (MBP1Schema) data);
      case BBO_1S -> fromBBO1S(timestamp, (BBO1SSchema) data);
      case BBO_1M -> fromBBO1M(timestamp, (BBO1MSchema) data);
      case TRADES -> fromTrades(timestamp, (TradesSchema) data);
      case MBO -> fromMBO(timestamp, (MBOSchema) data);
      case OHLCV_1S -> fromOHLCV1S(timestamp, (OHLCV1SSchema) data);
      case OHLCV_1M -> fromOHLCV1M(timestamp, (OHLCV1MSchema) data);
      case OHLCV_1H -> fromOHLCV1H(timestamp, (OHLCV1HSchema) data);
    };
    marketRecords.add(record);
  }

  public void onExecutionReport(long timestamp, BacktestExecutionReport report) {
    executionRecords.add(new ExecutionRecord(
        report.timestampEvent,
        report.timestampRecv,
        report.exchangeId,
        report.securityId,
        report.clientOid,
        report.side.name(),
        report.execType.name(),
        report.orderStatus.name(),
        report.filledQty,
        report.fillPrice,
        report.cumulativeQty,
        report.leavesQty,
        report.fee));
  }

  public List<MarketRecord> getMarketRecords() {
    return marketRecords;
  }

  public List<ExecutionRecord> getExecutionRecords() {
    return executionRecords;
  }

  public int getMarketRecordCount() {
    return marketRecords.size();
  }

  public int getExecutionRecordCount() {
    return executionRecords.size();
  }

  public void clear() {
    marketRecords.clear();
    executionRecords.clear();
  }

  // --- Schema-specific extraction ---

  private static MarketRecord fromMBP10(long timestamp, MBP10Schema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.bidPrice0(), d.askPrice0(), d.bidSize0(), d.askSize0(),
        d.price(), d.size(), d.sequence());
  }

  private static MarketRecord fromMBP1(long timestamp, MBP1Schema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.bidPrice0(), d.askPrice0(), d.bidSize0(), d.askSize0(),
        d.price(), d.size(), d.sequence());
  }

  private static MarketRecord fromBBO1S(long timestamp, BBO1SSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.bidPrice0(), d.askPrice0(), d.bidSize0(), d.askSize0(),
        d.price(), d.size(), d.sequence());
  }

  private static MarketRecord fromBBO1M(long timestamp, BBO1MSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.bidPrice0(), d.askPrice0(), d.bidSize0(), d.askSize0(),
        d.price(), d.size(), d.sequence());
  }

  private static MarketRecord fromTrades(long timestamp, TradesSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        0, 0, 0, 0,
        d.price(), d.size(), d.sequence());
  }

  private static MarketRecord fromMBO(long timestamp, MBOSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        0, 0, 0, 0,
        d.price(), d.size(), d.sequence());
  }

  private static MarketRecord fromOHLCV1S(long timestamp, OHLCV1SSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.close(), d.close(), 0, 0,
        d.close(), 0, 0);
  }

  private static MarketRecord fromOHLCV1M(long timestamp, OHLCV1MSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.close(), d.close(), 0, 0,
        d.close(), 0, 0);
  }

  private static MarketRecord fromOHLCV1H(long timestamp, OHLCV1HSchema s) {
    var d = s.decoder;
    return new MarketRecord(
        timestamp, d.exchangeId(), d.securityId(),
        d.close(), d.close(), 0, 0,
        d.close(), 0, 0);
  }
}
