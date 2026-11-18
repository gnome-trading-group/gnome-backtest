package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.config.BacktestConfig;
import group.gnometrading.backtest.config.BacktestContext;
import group.gnometrading.backtest.config.BacktestDriverFactory;
import group.gnometrading.backtest.config.ListingSimConfig;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.backtest.rewards.BacktestRewards;
import group.gnometrading.md.Stream;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.pnl.PriceWriter;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.TimeInForce;
import group.gnometrading.schemas.book.Action;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.schemas.book.Mbp10Encoder;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sequencer.metrics.ColumnDef;
import group.gnometrading.sequencer.metrics.MetricRecorder;
import group.gnometrading.sequencer.metrics.RecordBuffer;
import group.gnometrading.simulation.book.SelfTradePrevention;
import group.gnometrading.simulation.book.TradePairing;
import group.gnometrading.simulation.config.ExchangeSimulationConfig;
import group.gnometrading.simulation.config.FeeModelConfig;
import group.gnometrading.simulation.config.LatencyConfig;
import group.gnometrading.simulation.config.QueueModelConfig;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import group.gnometrading.sm.incentive.IncentiveProgram;
import group.gnometrading.sm.incentive.IncentiveTimeline;
import group.gnometrading.strategies.PythonStrategyAgent;
import group.gnometrading.strategies.StrategyControl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A print on one outcome of a paired market moves the queue on the other outcome's exchange, so the rewards estimate
 * reads where our orders there now stand at the print, not at that outcome's next book.
 */
class BacktestDriverComplementQueueTest {

    private static final int EXCHANGE_ID = 1;
    private static final int SECURITY_YES = 51;
    private static final int SECURITY_NO = 52;
    private static final int LISTING_YES = 501;
    private static final int LISTING_NO = 502;
    private static final int STREAM_YES = 801;
    private static final int STREAM_NO = 802;
    private static final long DOLLAR = Statics.PRICE_SCALING_FACTOR;
    private static final long CENT = DOLLAR / 100;
    private static final long CONTRACT = Statics.SIZE_SCALING_FACTOR;
    private static final long PRICE_NULL = Mbp10Decoder.bidPrice0NullValue();
    private static final long SIZE_NULL = Mbp10Decoder.bidSize0NullValue();
    private static final long SECOND = 1_000_000_000L;
    private static final long T0 = 1_780_000_000L * SECOND;
    private static final long PERIOD = 10_000 * SECOND;
    private static final Exchange KALSHI = new Exchange(EXCHANGE_ID, "KALSHI", "Kalshi", "us-east-1");

    private static Listing listing(int listingId, int securityId, String ticker) {
        return new Listing(
                listingId,
                KALSHI,
                new Security(securityId, ticker, null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                ticker,
                ticker);
    }

    /** $100 over the period from T0, 1,000 contracts a side, no discount. */
    private static IncentiveTimeline program() {
        return new IncentiveTimeline(List.<IncentiveProgram[]>of(new IncentiveProgram[] {
            new IncentiveProgram(
                    "k1",
                    IncentiveProgram.IncentiveKind.LIQUIDITY,
                    "KXRT-VIO-60",
                    T0,
                    T0 + PERIOD,
                    100 * DOLLAR,
                    IncentiveProgram.RewardPer.PERIOD,
                    JsonNodeFactory.instance
                            .objectNode()
                            .put("targetSize", 1000)
                            .put("discountFactor", 1.0),
                    false,
                    T0)
        }));
    }

    private static final class Quiet implements PythonStrategyAgent.PythonStrategyCallback {
        @Override
        public List<Intent> onMarketData(StreamRecord data) {
            return List.of();
        }

        @Override
        public List<Intent> onExecutionReport(OrderExecutionReport report) {
            return List.of();
        }

        @Override
        public long simulateProcessingTime() {
            return 0;
        }

        @Override
        public void onInit(PositionView positionView, SecurityMaster securityMaster, StrategyControl control) {}

        @Override
        public void registerMetrics(MetricRecorder recorder) {}
    }

    private final SimulatedExchange no;
    private final BacktestRecorder recorder = new BacktestRecorder();
    private final BacktestRewards rewards = new BacktestRewards(recorder, PERIOD);
    private final BacktestDriver driver;

    BacktestDriverComplementQueueTest() {
        SecurityMaster securityMaster = mock(SecurityMaster.class);
        Listing yesListing = listing(LISTING_YES, SECURITY_YES, "KXRT-VIO-60");
        Listing noListing = listing(LISTING_NO, SECURITY_NO, "KXRT-VIO-60:no");
        lenient().when(securityMaster.getListing(LISTING_YES)).thenReturn(yesListing);
        lenient().when(securityMaster.getListing(LISTING_NO)).thenReturn(noListing);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_YES)).thenReturn(yesListing);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_NO)).thenReturn(noListing);
        lenient().when(securityMaster.getListingSpec(anyInt(), anyLong())).thenReturn(null);
        StreamDirectory streams = new StreamDirectory();
        streams.add(
                new Stream(STREAM_YES, 1, "KALSHI", "LISTING", LISTING_YES, null, "book", "US", 0, 0L, 0L, true),
                yesListing);
        streams.add(
                new Stream(STREAM_NO, 1, "KALSHI", "LISTING", LISTING_NO, null, "book", "US", 0, 0L, 0L, true),
                noListing);

        ExchangeSimulationConfig simulation = new ExchangeSimulationConfig(
                new FeeModelConfig.Static(),
                new LatencyConfig.Static(),
                new LatencyConfig.Static(),
                new LatencyConfig.Static(),
                new QueueModelConfig.RiskAverse(),
                SelfTradePrevention.CANCEL_INCOMING,
                TradePairing.OFF,
                false);
        SimulatedExchange yes = simulation.toSimulatedExchange(LISTING_YES, 7, securityMaster::getFeeTimeline);
        no = simulation.toSimulatedExchange(LISTING_NO, 8, securityMaster::getFeeTimeline);

        BacktestConfig config = new BacktestConfig();
        ListingSimConfig ls = new ListingSimConfig();
        ls.listingId = LISTING_YES;
        config.listings = List.of(ls);
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        OrderManagementSystem oms = BacktestDriverFactory.buildOms(config, securityMaster, null, context);
        SequencedRingBuffer<Mbp10Schema> priceFeed = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        PythonStrategyAgent strategy = PythonStrategyAgent.create(
                0, oms.getPositionTracker().createPositionView(0), securityMaster, new Quiet());
        PriceWriter priceWriter = new PriceWriter(context.priceBuffer(), context.priceRegistry(), streams, priceFeed);
        driver = new BacktestDriver(
                List.of(),
                strategy,
                streams,
                Map.of(EXCHANGE_ID, Map.of(SECURITY_YES, yes, SECURITY_NO, no)),
                Map.of(
                        yes,
                        MarketDataArrival.of(new LatencyConfig.Static(), 7),
                        no,
                        MarketDataArrival.of(new LatencyConfig.Static(), 8)),
                new OmsBacktestAdapter(oms, priceWriter, context.clock()),
                priceWriter,
                priceFeed,
                null,
                "test",
                null,
                false);

        assertTrue(rewards.bind(yesListing, program(), nanos -> CENT));
        assertTrue(rewards.bind(noListing, program(), nanos -> CENT));
        rewards.begin(T0);
        driver.setRewards(rewards);
        driver.setComplements(Map.of(LISTING_YES, LISTING_NO, LISTING_NO, LISTING_YES));
    }

    private static void clearLevels(Mbp10Encoder e) {
        e.bidPrice0(PRICE_NULL).bidSize0(SIZE_NULL).askPrice0(PRICE_NULL).askSize0(SIZE_NULL);
        e.bidPrice1(PRICE_NULL).bidSize1(SIZE_NULL).askPrice1(PRICE_NULL).askSize1(SIZE_NULL);
        e.bidPrice2(PRICE_NULL).bidSize2(SIZE_NULL).askPrice2(PRICE_NULL).askSize2(SIZE_NULL);
        e.bidPrice3(PRICE_NULL).bidSize3(SIZE_NULL).askPrice3(PRICE_NULL).askSize3(SIZE_NULL);
        e.bidPrice4(PRICE_NULL).bidSize4(SIZE_NULL).askPrice4(PRICE_NULL).askSize4(SIZE_NULL);
        e.bidPrice5(PRICE_NULL).bidSize5(SIZE_NULL).askPrice5(PRICE_NULL).askSize5(SIZE_NULL);
        e.bidPrice6(PRICE_NULL).bidSize6(SIZE_NULL).askPrice6(PRICE_NULL).askSize6(SIZE_NULL);
        e.bidPrice7(PRICE_NULL).bidSize7(SIZE_NULL).askPrice7(PRICE_NULL).askSize7(SIZE_NULL);
        e.bidPrice8(PRICE_NULL).bidSize8(SIZE_NULL).askPrice8(PRICE_NULL).askSize8(SIZE_NULL);
        e.bidPrice9(PRICE_NULL).bidSize9(SIZE_NULL).askPrice9(PRICE_NULL).askSize9(SIZE_NULL);
    }

    /** Everyone else's 2,000 at the one level a side. */
    private static Mbp10Schema book(int stream, long timestamp, long bidCents, long askCents) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder
                .streamId(stream)
                .timestampEvent(timestamp)
                .timestampRecv(timestamp)
                .action(Action.Add)
                .side(group.gnometrading.schemas.book.Side.None)
                .price(PRICE_NULL)
                .size(SIZE_NULL);
        levels(schema.encoder, bidCents, 2000, askCents, 2000);
        return schema;
    }

    /** A buy that lifted {@code contracts} of the ask, with the book as it left it. */
    private static Mbp10Schema buy(
            int stream, long timestamp, long bidCents, long askCents, long askLeft, long contracts) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder
                .streamId(stream)
                .timestampEvent(timestamp)
                .timestampRecv(timestamp)
                .action(Action.Trade)
                .side(group.gnometrading.schemas.book.Side.Bid)
                .price(askCents * CENT)
                .size(contracts * CONTRACT);
        levels(schema.encoder, bidCents, 2000, askCents, askLeft);
        return schema;
    }

    private static void levels(Mbp10Encoder e, long bidCents, long bidSize, long askCents, long askSize) {
        clearLevels(e);
        e.bidPrice0(bidCents * CENT)
                .bidSize0(bidSize * CONTRACT)
                .askPrice0(askCents * CENT)
                .askSize0(askSize * CONTRACT);
    }

    /** Our NO bid, sent straight to its exchange and told to the rewards estimate as the driver would. */
    private void restNoBid(long at, long cents, long contracts) {
        Order order = new Order();
        order.encoder
                .exchangeId((short) EXCHANGE_ID)
                .securityId(SECURITY_NO)
                .price(cents * CENT)
                .size(contracts * CONTRACT)
                .side(Side.Bid)
                .orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(1, 0);
        List<OrderExecutionReport> reports = new ArrayList<>(no.submitOrder(order, at));
        reports.addAll(no.processDue(at));
        for (OrderExecutionReport report : reports) {
            rewards.onReport(at, no, report);
        }
    }

    /** The NO listing's last reward row's {@code name} column. */
    private long lastOnNo(String name) {
        RecordBuffer buffer = recorder.getRewardRecords();
        int listingColumn = -1;
        int valueColumn = -1;
        for (ColumnDef column : buffer.getColumns()) {
            if (column.name().equals("listing_id")) {
                listingColumn = column.columnIndex();
            } else if (column.name().equals(name)) {
                valueColumn = column.columnIndex();
            }
        }
        for (int row = buffer.getCount() - 1; row >= 0; row--) {
            if (buffer.getIntColumn(listingColumn)[row] == LISTING_NO) {
                return buffer.getLongColumn(valueColumn)[row];
            }
        }
        throw new AssertionError("No reward row for the NO listing");
    }

    @Test
    void aPrintOnOneOutcomeMovesOurOrdersOnTheOtherUpTheQueueAtThePrint() {
        driver.prepareRecords(List.of(
                book(STREAM_YES, T0, 50, 52),
                book(STREAM_NO, T0, 48, 50),
                buy(STREAM_YES, T0 + PERIOD / 2, 50, 52, 500, 1500),
                book(STREAM_YES, T0 + PERIOD / 2 + SECOND, 50, 52)));
        driver.executeUntil(T0 + 1);
        // Joins behind everyone else's 2,000 at 48c: the YES ask at 52c.
        restNoBid(T0 + 1, 48, 500);
        driver.fullyExecute();
        driver.finishRewards(T0 + PERIOD);

        // The YES buy of 1,500 at 52c sold NO at 48c ahead of us, leaving 500 there; a second later others join the
        // YES ask behind us, back to 2,000. Only the print says where our bid now stands: the NO book has no record
        // after it. Queue reading: nothing for the first half, then our 500 of the 1,000 the ask side needs, at $50 a
        // side over the period.
        assertEquals(1250 * CENT, lastOnNo("gross_queue"), 1);
    }
}
