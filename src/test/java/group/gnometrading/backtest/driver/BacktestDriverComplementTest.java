package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.config.BacktestConfig;
import group.gnometrading.backtest.config.BacktestContext;
import group.gnometrading.backtest.config.BacktestDriverFactory;
import group.gnometrading.backtest.config.ListingSimConfig;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.md.Stream;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.pnl.PriceWriter;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.book.Action;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.schemas.book.Mbp10Encoder;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sequencer.metrics.MetricRecorder;
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
import group.gnometrading.strategies.PythonStrategyAgent;
import group.gnometrading.strategies.StrategyControl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * On a venue that matches the two outcomes of a market against each other but prints the trade once, the driver hands
 * each print on one outcome to the other outcome's exchange too; every other record reaches only its own listing.
 */
class BacktestDriverComplementTest {

    private static final int EXCHANGE_ID = 4;
    private static final int SECURITY_A = 41;
    private static final int SECURITY_B = 42;
    private static final int LISTING_A = 401;
    private static final int LISTING_B = 402;
    private static final int STREAM_A = 701;
    private static final int STREAM_B = 702;
    private static final long CENT = Statics.PRICE_SCALING_FACTOR / 100;
    private static final long UNIT = Statics.SIZE_SCALING_FACTOR;
    private static final long PRICE_NULL = Mbp10Decoder.bidPrice0NullValue();
    private static final long SIZE_NULL = Mbp10Decoder.bidSize0NullValue();
    private static final long MS = 1_000_000L;
    private static final long T0 = 1_780_000_000_000_000_000L;
    private static final Exchange PM_INTL = new Exchange(EXCHANGE_ID, "POLYMARKET_INTL", "Polymarket", "eu-west-1");

    private static Listing listing(int listingId, int securityId, String exchangeSecurityId) {
        return new Listing(
                listingId,
                PM_INTL,
                new Security(securityId, "SYM", null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                exchangeSecurityId,
                "SYM");
    }

    /** What reaches one listing's exchange, by kind, before it goes to the real exchange. */
    private static final class Counting implements SimulatedExchange {
        private final SimulatedExchange delegate;
        final List<Long> ownRecords = new ArrayList<>();
        final List<Long> complementTrades = new ArrayList<>();

        Counting(SimulatedExchange delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<OrderExecutionReport> submitOrder(Order order, long nowNanos) {
            return delegate.submitOrder(order, nowNanos);
        }

        @Override
        public List<OrderExecutionReport> cancelOrder(CancelOrder cancel, long nowNanos) {
            return delegate.cancelOrder(cancel, nowNanos);
        }

        @Override
        public List<OrderExecutionReport> modifyOrder(ModifyOrder modify, long nowNanos) {
            return delegate.modifyOrder(modify, nowNanos);
        }

        @Override
        public List<OrderExecutionReport> processDue(long nowNanos) {
            return delegate.processDue(nowNanos);
        }

        @Override
        public long nextDueNanos() {
            return delegate.nextDueNanos();
        }

        @Override
        public List<OrderExecutionReport> onMarketData(StreamRecord data) {
            ownRecords.add(data.getEventTimestamp());
            return delegate.onMarketData(data);
        }

        @Override
        public List<OrderExecutionReport> onComplementTrade(StreamRecord data) {
            complementTrades.add(data.getEventTimestamp());
            return delegate.onComplementTrade(data);
        }

        @Override
        public long simulateNetworkLatency() {
            return delegate.simulateNetworkLatency();
        }

        @Override
        public List<RecordType> getSupportedSchemas() {
            return delegate.getSupportedSchemas();
        }
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

    private final Counting a;
    private final Counting b;
    private final BacktestDriver driver;

    BacktestDriverComplementTest() {
        SecurityMaster securityMaster = mock(SecurityMaster.class);
        Listing listingA = listing(LISTING_A, SECURITY_A, "0xcond:1");
        Listing listingB = listing(LISTING_B, SECURITY_B, "0xcond:2");
        lenient().when(securityMaster.getListing(LISTING_A)).thenReturn(listingA);
        lenient().when(securityMaster.getListing(LISTING_B)).thenReturn(listingB);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_A)).thenReturn(listingA);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_B)).thenReturn(listingB);
        lenient().when(securityMaster.getListingSpec(anyInt(), anyLong())).thenReturn(null);
        StreamDirectory streams = new StreamDirectory();
        streams.add(
                new Stream(STREAM_A, 1, "POLYMARKET_INTL", "LISTING", LISTING_A, null, "book", "EU", 0, 0L, 0L, true),
                listingA);
        streams.add(
                new Stream(STREAM_B, 1, "POLYMARKET_INTL", "LISTING", LISTING_B, null, "book", "EU", 0, 0L, 0L, true),
                listingB);

        ExchangeSimulationConfig simulation = new ExchangeSimulationConfig(
                new FeeModelConfig.Static(),
                new LatencyConfig.Static(),
                new LatencyConfig.Static(),
                new LatencyConfig.Static(),
                new QueueModelConfig.RiskAverse(),
                SelfTradePrevention.CANCEL_INCOMING,
                TradePairing.within(Duration.ofMillis(100), Duration.ofMillis(25)),
                true);
        a = new Counting(simulation.toSimulatedExchange(LISTING_A, 7, securityMaster::getFeeTimeline));
        b = new Counting(simulation.toSimulatedExchange(LISTING_B, 8, securityMaster::getFeeTimeline));

        BacktestConfig config = new BacktestConfig();
        ListingSimConfig la = new ListingSimConfig();
        la.listingId = LISTING_A;
        config.listings = List.of(la);
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
                Map.of(EXCHANGE_ID, Map.of(SECURITY_A, a, SECURITY_B, b)),
                Map.of(
                        a,
                        MarketDataArrival.of(new LatencyConfig.Static(), 7),
                        b,
                        MarketDataArrival.of(new LatencyConfig.Static(), 8)),
                new OmsBacktestAdapter(oms, priceWriter, context.clock()),
                priceWriter,
                priceFeed,
                null,
                "test",
                null,
                false);
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

    private static Mbp10Schema book(int stream, long timestamp, long bid, long ask) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder
                .streamId(stream)
                .timestampEvent(timestamp)
                .timestampRecv(timestamp)
                .action(Action.Add)
                .side(group.gnometrading.schemas.book.Side.None)
                .price(PRICE_NULL)
                .size(SIZE_NULL);
        clearLevels(schema.encoder);
        schema.encoder.bidPrice0(bid).bidSize0(10 * UNIT).askPrice0(ask).askSize0(10 * UNIT);
        return schema;
    }

    private static Mbp10Schema trade(int stream, long timestamp, long price) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder
                .streamId(stream)
                .timestampEvent(timestamp)
                .timestampRecv(timestamp)
                .action(Action.Trade)
                .side(group.gnometrading.schemas.book.Side.Bid)
                .price(price)
                .size(UNIT);
        clearLevels(schema.encoder);
        return schema;
    }

    private void paired() {
        driver.setComplements(Map.of(LISTING_A, LISTING_B, LISTING_B, LISTING_A));
    }

    private void run(List<StreamRecord> records) {
        driver.prepareRecords(records);
        driver.fullyExecute();
    }

    private List<StreamRecord> market() {
        return List.of(
                book(STREAM_A, T0, 40 * CENT, 42 * CENT),
                book(STREAM_B, T0 + MS, 58 * CENT, 60 * CENT),
                trade(STREAM_B, T0 + 2 * MS, 60 * CENT),
                trade(STREAM_A, T0 + 3 * MS, 42 * CENT));
    }

    @Test
    void aPrintOnOneOutcomeAlsoReachesTheOthersExchangeAndBookRecordsDoNot() {
        paired();
        run(market());

        assertEquals(List.of(T0, T0 + 3 * MS), a.ownRecords, "A's own book and print");
        assertEquals(List.of(T0 + MS, T0 + 2 * MS), b.ownRecords, "B's own book and print");
        assertEquals(List.of(T0 + 2 * MS), a.complementTrades, "B's print, once");
        assertEquals(List.of(T0 + 3 * MS), b.complementTrades, "A's print, once");
    }

    @Test
    void unpairedEachExchangeSeesOnlyItsOwnListing() {
        run(market());

        assertTrue(a.complementTrades.isEmpty());
        assertTrue(b.complementTrades.isEmpty());
        assertEquals(2, a.ownRecords.size());
        assertEquals(2, b.ownRecords.size());
    }

    @Test
    void aPairNamingAListingTheRunDoesNotSimulateIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> driver.setComplements(Map.of(LISTING_A, 999)));
    }
}
