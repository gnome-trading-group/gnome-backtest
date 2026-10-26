package group.gnometrading.backtest.recorder;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.md.Stream;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderExecutionReportDecoder;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.TimeInForce;
import group.gnometrading.schemas.book.Action;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.sequencer.metrics.ColumnDef;
import group.gnometrading.sequencer.metrics.MetricStream;
import group.gnometrading.sequencer.metrics.RecordBuffer;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BacktestRecorderTest {

    static final long PRICE_NULL = Mbp10Decoder.priceNullValue();
    static final long SIZE_NULL = Mbp10Decoder.sizeNullValue();

    BacktestRecorder recorder;

    // Stream 7 carries listing 3: exchange 1, security 100, as the orders and reports below name it.
    private static final int STREAM_ID = 7;

    @BeforeEach
    void setUp() {
        recorder = new BacktestRecorder();
        StreamDirectory streams = new StreamDirectory();
        streams.add(
                new Stream(STREAM_ID, 1, "KALSHI", "LISTING", 3, null, "book", "us-east-1", 0, 0L, 0L, true),
                new Listing(
                        3,
                        new Exchange(1, "KALSHI", "Kalshi", "us-east-1"),
                        new Security(100, "X", null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                        "X",
                        "X"));
        recorder.configureStreams(streams);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    static Mbp10Schema makeBbo(int streamId, long bidPx, long bidSz, long askPx, long askSz) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.streamId(streamId);
        schema.encoder.action(Action.Add);
        schema.encoder.side(group.gnometrading.schemas.book.Side.None);
        schema.encoder.price(PRICE_NULL).size(SIZE_NULL);
        schema.encoder.bidPrice0(bidPx).bidSize0(bidSz).askPrice0(askPx).askSize0(askSz);
        schema.encoder
                .bidPrice1(PRICE_NULL)
                .bidSize1(SIZE_NULL)
                .askPrice1(PRICE_NULL)
                .askSize1(SIZE_NULL);
        schema.encoder
                .bidPrice2(PRICE_NULL)
                .bidSize2(SIZE_NULL)
                .askPrice2(PRICE_NULL)
                .askSize2(SIZE_NULL);
        schema.encoder
                .bidPrice3(PRICE_NULL)
                .bidSize3(SIZE_NULL)
                .askPrice3(PRICE_NULL)
                .askSize3(SIZE_NULL);
        schema.encoder
                .bidPrice4(PRICE_NULL)
                .bidSize4(SIZE_NULL)
                .askPrice4(PRICE_NULL)
                .askSize4(SIZE_NULL);
        schema.encoder
                .bidPrice5(PRICE_NULL)
                .bidSize5(SIZE_NULL)
                .askPrice5(PRICE_NULL)
                .askSize5(SIZE_NULL);
        schema.encoder
                .bidPrice6(PRICE_NULL)
                .bidSize6(SIZE_NULL)
                .askPrice6(PRICE_NULL)
                .askSize6(SIZE_NULL);
        schema.encoder
                .bidPrice7(PRICE_NULL)
                .bidSize7(SIZE_NULL)
                .askPrice7(PRICE_NULL)
                .askSize7(SIZE_NULL);
        schema.encoder
                .bidPrice8(PRICE_NULL)
                .bidSize8(SIZE_NULL)
                .askPrice8(PRICE_NULL)
                .askSize8(SIZE_NULL);
        schema.encoder
                .bidPrice9(PRICE_NULL)
                .bidSize9(SIZE_NULL)
                .askPrice9(PRICE_NULL)
                .askSize9(SIZE_NULL);
        return schema;
    }

    static Order makeLimitOrder(int exchangeId, long securityId, long price, long size, Side side, long clientOid) {
        Order order = new Order();
        order.encoder
                .exchangeId((short) exchangeId)
                .securityId(securityId)
                .price(price)
                .size(size)
                .side(side)
                .orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(clientOid, 0);
        return order;
    }

    static OrderExecutionReport makeExecReport(
            int exchangeId,
            long securityId,
            ExecType execType,
            OrderStatus orderStatus,
            long fillPrice,
            long filledQty,
            long cumulativeQty,
            long leavesQty,
            long fee,
            long clientOid) {
        OrderExecutionReport report = new OrderExecutionReport();
        report.encoder
                .exchangeId(exchangeId)
                .securityId(securityId)
                .execType(execType)
                .orderStatus(orderStatus)
                .fillPrice(fillPrice)
                .filledQty(filledQty)
                .cumulativeQty(cumulativeQty)
                .leavesQty(leavesQty)
                .fee(fee);
        report.encodeClientOid(clientOid, 0);
        return report;
    }

    // =========================================================================
    // RecordBuffer access
    // =========================================================================

    @Test
    void testBuiltInBuffersExist() {
        assertNotNull(recorder.getMarketRecords());
        assertNotNull(recorder.getOrderRecords());
        assertNotNull(recorder.getFillRecords());
        assertNotNull(recorder.getIntentRecords());
        assertEquals("market", recorder.getMarketRecords().getName());
        assertEquals("orders", recorder.getOrderRecords().getName());
        assertEquals("fills", recorder.getFillRecords().getName());
        assertEquals("intents", recorder.getIntentRecords().getName());
    }

    @Test
    void testInitialCountsAreZero() {
        assertEquals(0, recorder.getMarketRecordCount());
        assertEquals(0, recorder.getOrderRecordCount());
        assertEquals(0, recorder.getFillRecordCount());
        assertEquals(0, recorder.getIntentRecordCount());
    }

    // =========================================================================
    // Market data recording
    // =========================================================================

    @Test
    void testOnMarketDataRecordsBbo() {
        Mbp10Schema schema = makeBbo(STREAM_ID, 9_900_000L, 10L, 10_100_000L, 5L);
        recorder.onMarketData(12345L, schema);

        assertEquals(1, recorder.getMarketRecordCount());
        RecordBuffer mkt = recorder.getMarketRecords();
        // timestamp
        assertEquals(12345L, mkt.getLongColumn(0)[0]); // timestamp col index 0
    }

    @Test
    void marketRowsCarryTheStreamAndTheListingItCarries() {
        recorder.onMarketData(0L, makeBbo(STREAM_ID, 100L, 5L, 200L, 5L));
        recorder.onMarketData(1L, makeBbo(99, 100L, 5L, 200L, 5L));

        RecordBuffer mkt = recorder.getMarketRecords();
        int[] streamIds = intColumn(mkt, "stream_id");
        int[] listingIds = intColumn(mkt, "listing_id");
        assertEquals(STREAM_ID, streamIds[0]);
        assertEquals(3, listingIds[0]);
        assertEquals(99, streamIds[1]);
        assertEquals(0, listingIds[1], "a stream outside the run's directory carries no listing");
    }

    private static int[] intColumn(RecordBuffer buffer, String name) {
        return buffer.getIntColumn(buffer.getColumns().stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElseThrow()
                .columnIndex());
    }

    @Test
    void testMarketRecordsHasNoDerivedColumns() {
        recorder.onMarketData(0L, makeBbo(STREAM_ID, 9_900_000L, 10L, 10_100_000L, 5L));

        RecordBuffer mkt = recorder.getMarketRecords();
        boolean hasMid = mkt.getColumns().stream().anyMatch(c -> c.name().equals("mid_price"));
        boolean hasSpread = mkt.getColumns().stream().anyMatch(c -> c.name().equals("spread"));
        boolean hasImbalance = mkt.getColumns().stream().anyMatch(c -> c.name().equals("imbalance"));
        boolean hasBidLevels = mkt.getColumns().stream().anyMatch(c -> c.name().equals("bid_levels"));
        boolean hasAskLevels = mkt.getColumns().stream().anyMatch(c -> c.name().equals("ask_levels"));
        assertFalse(hasMid);
        assertFalse(hasSpread);
        assertFalse(hasImbalance);
        assertFalse(hasBidLevels);
        assertFalse(hasAskLevels);
    }

    @Test
    void testMultipleMarketDataRows() {
        recorder.onMarketData(100L, makeBbo(STREAM_ID, 100L, 5L, 200L, 5L));
        recorder.onMarketData(200L, makeBbo(STREAM_ID, 110L, 5L, 210L, 5L));
        recorder.onMarketData(300L, makeBbo(STREAM_ID, 120L, 5L, 220L, 5L));

        assertEquals(3, recorder.getMarketRecordCount());
    }

    // =========================================================================
    // Order recording
    // =========================================================================

    @Test
    void testOrderFillWritesOrderAndFillRecords() {
        long clientOid = 1L;
        long price = 10_000_000L;
        long size = 100L;
        long fee = 500L;

        // Establish BBO so fill can be annotated with book state
        recorder.onMarketData(0L, makeBbo(STREAM_ID, 9_900_000L, 10L, 10_100_000L, 5L));

        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, price, size, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, size, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.FILL, OrderStatus.FILLED, price, size, size, 0, fee, clientOid),
                0,
                Side.Bid);

        assertEquals(1, recorder.getOrderRecordCount());
        assertEquals(1, recorder.getFillRecordCount());
    }

    @Test
    void testPartialFillThenFullFillAccumulatesOrder() {
        long clientOid = 2L;
        long price = 10_000_000L;
        long totalSize = 100L;
        long partialSize = 40L;
        long remainingSize = 60L;

        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, price, totalSize, Side.Bid, clientOid));
        recorder.onExecution(
                1001L,
                makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, totalSize, 0, clientOid),
                0,
                Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(
                        1,
                        100,
                        ExecType.PARTIAL_FILL,
                        OrderStatus.PARTIALLY_FILLED,
                        price,
                        partialSize,
                        partialSize,
                        remainingSize,
                        0,
                        clientOid),
                0,
                Side.Bid);
        recorder.onExecution(
                1003L,
                makeExecReport(
                        1, 100, ExecType.FILL, OrderStatus.FILLED, price, remainingSize, totalSize, 0, 0, clientOid),
                0,
                Side.Bid);

        // Two fill records, one order record
        assertEquals(2, recorder.getFillRecordCount());
        assertEquals(1, recorder.getOrderRecordCount());
    }

    @Test
    void testCancelledOrderWritesOrderRecord() {
        long clientOid = 3L;

        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Ask, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 50L, 0, clientOid), 0, Side.Ask);
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.CANCEL, OrderStatus.CANCELED, 0, 0, 0, 50L, 0, clientOid),
                0,
                Side.Ask);

        assertEquals(1, recorder.getOrderRecordCount());
        assertEquals(0, recorder.getFillRecordCount());

        // Verify final status byte
        RecordBuffer orders = recorder.getOrderRecords();
        byte statusByte = 0;
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals("final_status")) {
                statusByte = orders.getByteColumn(col.columnIndex())[0];
            }
        }
        assertEquals(BacktestRecorder.STATUS_CANCELLED, statusByte);
    }

    /** The value of a long column in the first order record. */
    private long orderLong(String column) {
        RecordBuffer orders = recorder.getOrderRecords();
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals(column)) {
                return orders.getLongColumn(col.columnIndex())[0];
            }
        }
        throw new AssertionError("no column " + column);
    }

    private int orderInt(String column) {
        RecordBuffer orders = recorder.getOrderRecords();
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals(column)) {
                return orders.getIntColumn(col.columnIndex())[0];
            }
        }
        throw new AssertionError("no column " + column);
    }

    private static ModifyOrder makeModify(long clientOid, long price, long size) {
        ModifyOrder modify = new ModifyOrder();
        modify.encoder.exchangeId((short) 1).securityId(100).price(price).size(size);
        modify.encodeClientOid(clientOid, 0);
        return modify;
    }

    private static CancelOrder makeCancel(long clientOid) {
        CancelOrder cancel = new CancelOrder();
        cancel.encoder.exchangeId((short) 1).securityId(100);
        cancel.encodeClientOid(clientOid, 0);
        return cancel;
    }

    @Test
    void testCancelRejectLeavesTheOrderWorkingAndLaterFillsCount() {
        long clientOid = 7L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 50L, 0, clientOid), 0, Side.Bid);
        recorder.onCancelSubmitted(makeCancel(clientOid));
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.CANCEL_REJECT, OrderStatus.NEW, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);
        assertEquals(0, recorder.getOrderRecordCount());

        recorder.onExecution(
                1003L,
                makeExecReport(1, 100, ExecType.FILL, OrderStatus.FILLED, 10_000_000L, 50L, 50L, 0, 0, clientOid),
                0,
                Side.Bid);

        assertEquals(1, recorder.getOrderRecordCount());
        assertEquals(50L, orderLong("filled_qty"));
    }

    @Test
    void testAcknowledgedModifyUpdatesFinalPriceAndSize() {
        long clientOid = 8L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 50L, 0, clientOid), 0, Side.Bid);
        recorder.onModifySubmitted(makeModify(clientOid, 11_000_000L, 60L));
        recorder.onExecution(
                1002L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 60L, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1003L,
                makeExecReport(1, 100, ExecType.CANCEL, OrderStatus.CANCELED, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);

        assertEquals(10_000_000L, orderLong("submit_price"));
        assertEquals(50L, orderLong("submit_size"));
        assertEquals(11_000_000L, orderLong("final_price"));
        assertEquals(60L, orderLong("final_size"));
        assertEquals(1, orderInt("modify_count"));
        // The first NEW is still the order's acknowledgement.
        assertEquals(1001L, orderLong("ack_timestamp"));
    }

    @Test
    void testRefusedCancelDoesNotConsumeAPendingModify() {
        long clientOid = 9L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 50L, 0, clientOid), 0, Side.Bid);
        recorder.onCancelSubmitted(makeCancel(clientOid));
        recorder.onModifySubmitted(makeModify(clientOid, 12_000_000L, 40L));
        // The venue refuses the cancel, then acknowledges the modify.
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.CANCEL_REJECT, OrderStatus.NEW, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);
        recorder.onExecution(
                1003L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 40L, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1004L,
                makeExecReport(1, 100, ExecType.CANCEL, OrderStatus.CANCELED, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);

        assertEquals(12_000_000L, orderLong("final_price"));
        assertEquals(1, orderInt("modify_count"));
    }

    @Test
    void testRefusedModifyLeavesFinalPriceUnchanged() {
        long clientOid = 10L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 50L, 0, clientOid), 0, Side.Bid);
        recorder.onModifySubmitted(makeModify(clientOid, 12_000_000L, 40L));
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.CANCEL_REJECT, OrderStatus.NEW, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);
        recorder.onExecution(
                1003L,
                makeExecReport(1, 100, ExecType.CANCEL, OrderStatus.CANCELED, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);

        assertEquals(10_000_000L, orderLong("final_price"));
        assertEquals(50L, orderLong("final_size"));
        assertEquals(0, orderInt("modify_count"));
    }

    @Test
    void testOrdersStillWorkingAtTheEndAreRecordedAsOpen() {
        long clientOid = 11L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 50L, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(
                        1,
                        100,
                        ExecType.PARTIAL_FILL,
                        OrderStatus.PARTIALLY_FILLED,
                        10_000_000L,
                        20L,
                        20L,
                        30L,
                        0,
                        clientOid),
                0,
                Side.Bid);
        assertEquals(0, recorder.getOrderRecordCount());

        recorder.closeOpenOrders(5000L);

        assertEquals(1, recorder.getOrderRecordCount());
        assertEquals(30L, orderLong("leaves_qty"));
        assertEquals(20L, orderLong("filled_qty"));
        assertEquals(5000L, orderLong("terminal_timestamp"));
        RecordBuffer orders = recorder.getOrderRecords();
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals("final_status")) {
                assertEquals(BacktestRecorder.STATUS_OPEN, orders.getByteColumn(col.columnIndex())[0]);
            }
        }
    }

    @Test
    void testRejectedOrderWritesOrderRecord() {
        long clientOid = 4L;

        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 10_000_000L, 50L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L,
                makeExecReport(1, 100, ExecType.REJECT, OrderStatus.REJECTED, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Bid);

        assertEquals(1, recorder.getOrderRecordCount());
        RecordBuffer orders = recorder.getOrderRecords();
        byte statusByte = 0;
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals("final_status")) {
                statusByte = orders.getByteColumn(col.columnIndex())[0];
            }
        }
        assertEquals(BacktestRecorder.STATUS_REJECTED, statusByte);
    }

    @Test
    void testFillAnnotatedWithBboAtFillTime() {
        long bidPx = 9_900_000L;
        long askPx = 10_100_000L;
        recorder.onMarketData(0L, makeBbo(STREAM_ID, bidPx, 10L, askPx, 5L));

        long clientOid = 5L;
        long fillPrice = 10_050_000L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, fillPrice, 10L, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 10L, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.FILL, OrderStatus.FILLED, fillPrice, 10L, 10L, 0, 0, clientOid),
                0,
                Side.Bid);

        RecordBuffer fills = recorder.getFillRecords();
        long bookBid = 0L;
        long bookAsk = 0L;
        boolean hasBookMid = fills.getColumns().stream().anyMatch(c -> c.name().equals("book_mid_price"));
        for (ColumnDef col : fills.getColumns()) {
            switch (col.name()) {
                case "book_bid_price" -> bookBid = fills.getLongColumn(col.columnIndex())[0];
                case "book_ask_price" -> bookAsk = fills.getLongColumn(col.columnIndex())[0];
                default -> {}
            }
        }
        assertEquals(bidPx, bookBid);
        assertEquals(askPx, bookAsk);
        assertFalse(hasBookMid);
    }

    @Test
    void testUnknownClientOidFillRecordedButNoOrderRecord() {
        // A FILL for an unknown clientOid still writes a fill record (the exchange fill happened),
        // but there is no in-flight order to finalize so the order record count stays zero.
        recorder.onExecution(
                1000L,
                makeExecReport(1, 100, ExecType.FILL, OrderStatus.FILLED, 100L, 10L, 10L, 0L, 0L, 999L),
                0,
                Side.Bid);

        assertEquals(1, recorder.getFillRecordCount());
        assertEquals(0, recorder.getOrderRecordCount());
    }

    // =========================================================================
    // Intent recording
    // =========================================================================

    @Test
    void testOnIntentRecordsEntry() {
        Intent intent = new Intent();
        intent.encoder
                .exchangeId(1)
                .securityId(100)
                .strategyId(0)
                .bidPrice(9_900_000L)
                .bidSize(10L)
                .askPrice(10_100_000L)
                .askSize(10L);

        recorder.onIntent(5000L, intent);

        assertEquals(1, recorder.getIntentRecordCount());
        RecordBuffer intents = recorder.getIntentRecords();
        long bidPrice = 0L;
        for (ColumnDef col : intents.getColumns()) {
            if (col.name().equals("bid_price")) {
                bidPrice = intents.getLongColumn(col.columnIndex())[0];
            }
        }
        assertEquals(9_900_000L, bidPrice);
    }

    // =========================================================================
    // Custom buffers
    // =========================================================================

    @Test
    void testEachStrategysMetricStreamsJoinTheCustomBuffers() {
        MetricStream alpha = recorder.createMetricRecorder().createStream("alpha");
        int signal = alpha.addDoubleColumn("signal");
        alpha.freeze();
        MetricStream beta = recorder.createMetricRecorder().createStream("beta");
        beta.addLongColumn("timestamp");
        beta.freeze();

        alpha.begin();
        alpha.setDouble(signal, 1.23);
        alpha.commit();

        assertEquals(
                List.of("alpha", "beta"),
                recorder.getCustomBuffers().stream().map(RecordBuffer::getName).toList());
        assertEquals(1.23, recorder.getCustomBuffers().get(0).getDoubleColumn(0)[0], 1e-10);
    }

    // =========================================================================
    // Clear
    // =========================================================================

    @Test
    void testClearResetsAllBuiltInStreams() {
        recorder.onMarketData(0L, makeBbo(STREAM_ID, 100L, 5L, 200L, 5L));
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 100L, 10L, Side.Bid, 1L));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 10L, 0, 1L), 0, Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.FILL, OrderStatus.FILLED, 100L, 10L, 10L, 0L, 0L, 1L),
                0,
                Side.Bid);

        Intent intent = new Intent();
        intent.encoder.exchangeId(1).securityId(100).strategyId(0);
        recorder.onIntent(0L, intent);

        recorder.clear();

        assertEquals(0, recorder.getMarketRecordCount());
        assertEquals(0, recorder.getOrderRecordCount());
        assertEquals(0, recorder.getFillRecordCount());
        assertEquals(0, recorder.getIntentRecordCount());
    }

    // =========================================================================
    // Record depth
    // =========================================================================

    @Test
    void testRecordDepthDefaultIsOne() {
        assertEquals(1, recorder.getRecordDepth());
    }

    @Test
    void testRecordDepthTwo() {
        BacktestRecorder depthTwo = new BacktestRecorder(2);
        assertEquals(2, depthTwo.getRecordDepth());
        // Should have bid_price_0 and bid_price_1 columns but not bid_price_2
        RecordBuffer mkt = depthTwo.getMarketRecords();
        boolean hasBid1 = mkt.getColumns().stream().anyMatch(c -> c.name().equals("bid_price_1"));
        boolean hasBid2 = mkt.getColumns().stream().anyMatch(c -> c.name().equals("bid_price_2"));
        assertTrue(hasBid1);
        assertFalse(hasBid2);
    }

    @Test
    void testInvalidRecordDepthThrows() {
        assertThrows(IllegalArgumentException.class, () -> new BacktestRecorder(0));
        assertThrows(IllegalArgumentException.class, () -> new BacktestRecorder(11));
    }

    @Test
    void testOrderSideEncodedCorrectly() {
        long clientOid = 10L;
        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, 100L, 10L, Side.Ask, clientOid));
        recorder.onExecution(
                1001L,
                makeExecReport(1, 100, ExecType.REJECT, OrderStatus.REJECTED, 0, 0, 0, 0, 0, clientOid),
                0,
                Side.Ask);

        RecordBuffer orders = recorder.getOrderRecords();
        byte sideByte = 0;
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals("side")) {
                sideByte = orders.getByteColumn(col.columnIndex())[0];
            }
        }
        assertEquals(BacktestRecorder.SIDE_ASK, sideByte);
    }

    @Test
    void testOrderTimestampsAreRecorded() {
        long submitTs = 1000L;
        long ackTs = 1001L;
        long terminalTs = 1002L;
        long clientOid = 20L;

        recorder.onOrderSubmitted(submitTs, makeLimitOrder(1, 100, 100L, 10L, Side.Bid, clientOid));
        recorder.onExecution(
                ackTs, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, 10L, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                terminalTs,
                makeExecReport(1, 100, ExecType.CANCEL, OrderStatus.CANCELED, 0, 0, 0, 10L, 0, clientOid),
                0,
                Side.Bid);

        RecordBuffer orders = recorder.getOrderRecords();
        long recSubmitTs = 0L;
        long recAckTs = 0L;
        long recTerminalTs = 0L;
        for (ColumnDef col : orders.getColumns()) {
            switch (col.name()) {
                case "submit_timestamp" -> recSubmitTs = orders.getLongColumn(col.columnIndex())[0];
                case "ack_timestamp" -> recAckTs = orders.getLongColumn(col.columnIndex())[0];
                case "terminal_timestamp" -> recTerminalTs = orders.getLongColumn(col.columnIndex())[0];
                default -> {}
            }
        }
        assertEquals(submitTs, recSubmitTs);
        assertEquals(ackTs, recAckTs);
        assertEquals(terminalTs, recTerminalTs);
    }

    @Test
    void testFillTotalCostIsAccumulated() {
        long clientOid = 30L;
        long size = 100L;
        long price = 10_000_000L;

        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, price, size, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, size, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(1, 100, ExecType.FILL, OrderStatus.FILLED, price, size, size, 0, 0, clientOid),
                0,
                Side.Bid);

        RecordBuffer orders = recorder.getOrderRecords();
        long totalCost = 0L;
        long filledQty = 0L;
        for (ColumnDef col : orders.getColumns()) {
            if (col.name().equals("total_cost")) {
                totalCost = orders.getLongColumn(col.columnIndex())[0];
            } else if (col.name().equals("filled_qty")) {
                filledQty = orders.getLongColumn(col.columnIndex())[0];
            }
        }
        assertEquals(price * size / Statics.SIZE_SCALING_FACTOR, totalCost, "notional in price units");
        assertEquals(size, filledQty);
    }

    @Test
    void testFillWithoutAFeeRecordsZeroFee() {
        long clientOid = 31L;
        long size = 100L;
        long price = 10_000_000L;

        recorder.onOrderSubmitted(1000L, makeLimitOrder(1, 100, price, size, Side.Bid, clientOid));
        recorder.onExecution(
                1001L, makeExecReport(1, 100, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, size, 0, clientOid), 0, Side.Bid);
        recorder.onExecution(
                1002L,
                makeExecReport(
                        1,
                        100,
                        ExecType.FILL,
                        OrderStatus.FILLED,
                        price,
                        size,
                        size,
                        0,
                        OrderExecutionReportDecoder.feeNullValue(),
                        clientOid),
                0,
                Side.Bid);

        assertEquals(0.0, doubleColumn(recorder.getFillRecords(), "fee")[0]);
        assertEquals(0.0, doubleColumn(recorder.getOrderRecords(), "total_fee")[0]);
    }

    private static double[] doubleColumn(RecordBuffer buffer, String name) {
        for (ColumnDef col : buffer.getColumns()) {
            if (col.name().equals(name)) {
                return buffer.getDoubleColumn(col.columnIndex());
            }
        }
        throw new IllegalArgumentException("No column " + name);
    }
}
