package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.config.BacktestConfig;
import group.gnometrading.backtest.config.BacktestContext;
import group.gnometrading.backtest.config.BacktestDriverFactory;
import group.gnometrading.backtest.config.ListingSimConfig;
import group.gnometrading.backtest.config.RiskConfig;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.pnl.PriceWriterAgent;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.RejectReason;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.simulation.config.ExchangeProfileConfig;
import group.gnometrading.simulation.config.LatencyConfig;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.Security;
import group.gnometrading.strategies.PythonStrategyAgent;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * How the driver times messages: each connection delivers in send order whatever latency each message draws, and the
 * strategy handles one event at a time. Runs the real OMS and exchange; the exchange is only wrapped to see what
 * reaches it and when.
 */
class BacktestDriverSchedulingTest {

    private static final int EXCHANGE_ID = 1;
    private static final int SECURITY_ID = 42;
    private static final int LISTING_ID = 100;
    private static final long CENT = Statics.PRICE_SCALING_FACTOR / 100;
    private static final long UNIT = Statics.SIZE_SCALING_FACTOR;
    private static final long PRICE_NULL = Mbp10Decoder.bidPrice0NullValue();
    private static final long SIZE_NULL = Mbp10Decoder.bidSize0NullValue();
    private static final long US = 1_000L;
    private static final long MS = 1_000_000L;
    private static final long SEC = 1_000_000_000L;
    private static final String STREAM_BUCKET = "test";
    private static final long T0 = 1_780_000_000_000_000_000L;

    @Test
    void ordersAndCancelsReachTheExchangeInSendOrderDespiteJitter() {
        // Each tick takes far from the book, and a bid is quoted and pulled ten ticks apart, so orders and cancels
        // interleave. The OMS holds a pull until the quote is acked, so the gap lets most of them go out.
        ScriptedStrategy strategy = new ScriptedStrategy(0, tick -> {
            Intent intent = takeIntent(Side.Bid, UNIT, 10 * CENT);
            if (tick % 20 < 10) {
                intent.encoder.bidPrice(20 * CENT).bidSize(UNIT);
            } else {
                intent.encoder.bidPrice(0).bidSize(0);
            }
            return List.of(intent);
        });
        Harness h = new Harness(jitter(), strategy, new RiskConfig());

        h.run(ticks(300, MS, 50 * CENT, 10 * UNIT));

        assertTrue(h.exchange.newOrders.size() > 300, "the scenario sends plenty of orders");
        assertTrue(h.exchange.cancels > 10, "and some cancels");
        long previous = 0;
        for (long counter : h.exchange.newOrders) {
            assertTrue(counter > previous, "new orders reach the exchange in the order they were sent");
            previous = counter;
        }
        assertEquals(0, h.exchange.beforeTheirOrder, "no cancel or modify overtakes the order it refers to");
    }

    @Test
    void reportsReachTheStrategyInTheOrderTheExchangeSentThemDespiteJitter() {
        // Taking 5 against 3 shown fills 3 and cancels the rest: a partial fill and a cancel sent back to back.
        ScriptedStrategy strategy = new ScriptedStrategy(0, tick -> List.of(takeIntent(Side.Bid, 5 * UNIT, 52 * CENT)));
        Harness h = new Harness(jitter(), strategy, new RiskConfig());

        // The ask moves between two prices, so each tick shows fresh liquidity.
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            records.add(book(T0 + i * MS, (50 + i % 2) * CENT, 3 * UNIT));
        }
        h.run(records);

        assertTrue(h.exchange.filled > 100 * UNIT, "the scenario fills plenty");
        assertEquals(0, strategy.afterFinal, "no order's partial fill reaches the strategy after its cancel");
        assertEquals(h.exchange.filled, strategy.filled, "the strategy hears of every fill the exchange made");
    }

    @Test
    void eventsArrivingWhileTheStrategyIsBusyWaitTheirTurn() {
        long latency = MS;
        long processing = 50 * US;
        ScriptedStrategy strategy =
                new ScriptedStrategy(processing, tick -> List.of(takeIntent(Side.Bid, UNIT, 10 * CENT)));
        Harness h = new Harness(fixed(latency), strategy, new RiskConfig());

        // Updates every 10us, faster than the strategy's 50us per update.
        h.run(ticks(5, 10 * US, 50 * CENT, 10 * UNIT));

        assertEquals(5, h.exchange.arrivals.size());
        for (int i = 0; i < 5; i++) {
            // Update i is taken up once update i-1 is done, and its order leaves when it is done in turn.
            long expected = T0 + latency + (i + 1) * processing + latency;
            assertEquals(expected, h.exchange.arrivals.get(i), "order " + i);
        }
    }

    @Test
    void aRiskRejectQueuesBehindWaitingUpdatesAndTakesProcessingTime() {
        long latency = MS;
        long processing = 50 * US;
        RiskConfig risk = new RiskConfig();
        RiskConfig.PolicyConfig maxSize = new RiskConfig.PolicyConfig();
        maxSize.type = "MAX_ORDER_SIZE";
        maxSize.params = Map.of("maxOrderSize", 5 * UNIT);
        risk.policies.add(maxSize);
        // The first update sends an order over the limit; the OMS's reject is answered with one under it.
        ScriptedStrategy strategy = new ScriptedStrategy(
                processing, tick -> tick == 0 ? List.of(takeIntent(Side.Bid, 10 * UNIT, 10 * CENT)) : List.of());
        strategy.onRiskReject = () -> List.of(takeIntent(Side.Bid, UNIT, 10 * CENT));
        Harness h = new Harness(fixed(latency), strategy, risk);

        h.run(ticks(2, 10 * US, 50 * CENT, 10 * UNIT));

        // Update 0 ends at +50us and is rejected then; update 1 waited since +10us so goes first, ending at +100us;
        // the reject is handled next, ending at +150us, when the replacement leaves.
        assertEquals(List.of(T0 + latency + 3 * processing + latency), h.exchange.arrivals);
    }

    @Test
    void aStrategyAnsweringEveryRejectWithAnotherStillLetsTheRunEnd() {
        RiskConfig risk = new RiskConfig();
        RiskConfig.PolicyConfig maxSize = new RiskConfig.PolicyConfig();
        maxSize.type = "MAX_ORDER_SIZE";
        maxSize.params = Map.of("maxOrderSize", 5 * UNIT);
        risk.policies.add(maxSize);
        ScriptedStrategy strategy =
                new ScriptedStrategy(50 * US, tick -> List.of(takeIntent(Side.Bid, 10 * UNIT, 10 * CENT)));
        strategy.onRiskReject = () -> List.of(takeIntent(Side.Bid, 10 * UNIT, 10 * CENT));
        Harness h = new Harness(fixed(MS), strategy, risk);

        h.run(ticks(3, MS, 50 * CENT, 10 * UNIT));

        assertTrue(h.exchange.arrivals.isEmpty(), "every order was over the limit");
    }

    @Test
    void withNoProcessingTimeOrdersLeaveAsTheUpdateArrives() {
        long latency = MS;
        ScriptedStrategy strategy = new ScriptedStrategy(0, tick -> List.of(takeIntent(Side.Bid, UNIT, 10 * CENT)));
        Harness h = new Harness(fixed(latency), strategy, new RiskConfig());

        h.run(ticks(3, 10 * US, 50 * CENT, 10 * UNIT));

        assertEquals(
                List.of(T0 + 2 * latency, T0 + 10 * US + 2 * latency, T0 + 20 * US + 2 * latency), h.exchange.arrivals);
    }

    @Test
    void recordedMarketDataArrivesWhenItWasReceivedAndInOrder() {
        long latency = MS;
        ScriptedStrategy strategy = new ScriptedStrategy(0, tick -> List.of(takeIntent(Side.Bid, UNIT, 10 * CENT)));
        Harness h = new Harness(new LatencyConfig.Recorded(), fixed(latency), strategy, new RiskConfig());

        // The third record was received before the second: it can't overtake it on the connection.
        h.run(List.of(
                book(T0, T0 + 30 * MS, 50 * CENT, 10 * UNIT),
                book(T0 + MS, T0 + 80 * MS, 50 * CENT, 10 * UNIT),
                book(T0 + 2 * MS, T0 + 7 * MS, 50 * CENT, 10 * UNIT)));

        assertEquals(
                List.of(T0 + 30 * MS + latency, T0 + 80 * MS + latency, T0 + 80 * MS + latency), h.exchange.arrivals);
    }

    @Test
    void recordedMarketDataWithoutAReceiveTimeUsesTheFallback() {
        long latency = MS;
        ScriptedStrategy strategy = new ScriptedStrategy(0, tick -> List.of(takeIntent(Side.Bid, UNIT, 10 * CENT)));
        LatencyConfig.Recorded recorded = new LatencyConfig.Recorded();
        recorded.fallback = fixed(20 * MS);
        Harness h = new Harness(recorded, fixed(latency), strategy, new RiskConfig());

        // No receive time, and one before its own event: neither is a time the record could have arrived.
        h.run(List.of(book(T0, 0, 50 * CENT, 10 * UNIT), book(T0 + 50 * MS, T0, 50 * CENT, 10 * UNIT)));

        assertEquals(List.of(T0 + 20 * MS + latency, T0 + 70 * MS + latency), h.exchange.arrivals);
    }

    @Test
    void streamedMarketDataRunsExactlyAsDataLoadedUpFront() throws IOException {
        LocalDateTime start = LocalDateTime.of(2026, 8, 24, 0, 0);
        long t0 = start.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        InMemoryS3 s3 = new InMemoryS3();
        List<MarketDataEntry> entries = new ArrayList<>();
        for (int minute = 0; minute < 5; minute++) {
            entries.add(new MarketDataEntry(
                    SECURITY_ID,
                    EXCHANGE_ID,
                    SchemaType.MBP_10,
                    start.plusMinutes(minute),
                    MarketDataEntry.EntryType.AGGREGATED));
        }
        // Out of order inside a file, one record filed a minute late, an empty minute, and one filed too late to
        // replay: the run has passed its time by the time its file loads.
        List<Schema> minute0 = List.of(
                book(t0 + 10 * SEC, 50 * CENT, UNIT),
                book(t0 + 5 * SEC, 50 * CENT, UNIT),
                book(t0 + 55 * SEC, 50 * CENT, UNIT));
        List<Schema> minute1 = List.of(book(t0 + 50 * SEC, 50 * CENT, UNIT), book(t0 + 70 * SEC, 50 * CENT, UNIT));
        List<Schema> minute3 = List.of(book(t0 + 190 * SEC, 50 * CENT, UNIT));
        Mbp10Schema tooLate = book(t0 + 30 * SEC, 50 * CENT, UNIT);
        List<Schema> minute4 = List.of(tooLate, book(t0 + 250 * SEC, 50 * CENT, UNIT));
        entries.get(0).saveToS3(s3, STREAM_BUCKET, minute0);
        entries.get(1).saveToS3(s3, STREAM_BUCKET, minute1);
        entries.get(3).saveToS3(s3, STREAM_BUCKET, minute3);
        entries.get(4).saveToS3(s3, STREAM_BUCKET, minute4);

        Harness upFront = new Harness(fixed(MS), new ScriptedStrategy(0, tick -> List.of(take())), new RiskConfig());
        List<Schema> everythingButTooLate = new ArrayList<>();
        everythingButTooLate.addAll(minute0);
        everythingButTooLate.addAll(minute1);
        everythingButTooLate.addAll(minute3);
        everythingButTooLate.add(minute4.get(1));
        upFront.run(everythingButTooLate);
        Harness streamed = new Harness(fixed(MS), new ScriptedStrategy(0, tick -> List.of(take())), new RiskConfig());
        streamed.runStreamed(entries, s3);

        assertEquals(7, upFront.exchange.arrivals.size());
        assertEquals(upFront.exchange.arrivals, streamed.exchange.arrivals);
    }

    private static Intent take() {
        return takeIntent(Side.Bid, UNIT, 10 * CENT);
    }

    private static LatencyConfig jitter() {
        LatencyConfig.LogNormal jitter = new LatencyConfig.LogNormal();
        jitter.floorNanos = 0;
        jitter.medianNanos = MS;
        jitter.p99Nanos = 10 * MS;
        return jitter;
    }

    private static LatencyConfig fixed(long nanos) {
        LatencyConfig.Static fixed = new LatencyConfig.Static();
        fixed.latencyNanos = nanos;
        return fixed;
    }

    private static List<Schema> ticks(int count, long spacing, long askPrice, long askSize) {
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(book(T0 + i * spacing, askPrice, askSize));
        }
        return records;
    }

    private static Mbp10Schema book(long timestamp, long askPrice, long askSize) {
        return book(timestamp, timestamp, askPrice, askSize);
    }

    private static Mbp10Schema book(long timestamp, long received, long askPrice, long askSize) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder
                .exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .timestampEvent(timestamp)
                .timestampRecv(received)
                .action(Action.Add)
                .side(Side.None)
                .price(PRICE_NULL)
                .size(SIZE_NULL);
        schema.encoder
                .bidPrice0(PRICE_NULL)
                .bidSize0(SIZE_NULL)
                .askPrice0(askPrice)
                .askSize0(askSize);
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

    private static Intent takeIntent(Side side, long size, long limitPrice) {
        Intent intent = new Intent();
        intent.encoder
                .strategyId(0)
                .exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .bidPrice(IntentDecoder.bidPriceNullValue())
                .bidSize(IntentDecoder.bidSizeNullValue())
                .askPrice(IntentDecoder.askPriceNullValue())
                .askSize(IntentDecoder.askSizeNullValue())
                .takeSize(size)
                .takeSide(side)
                .takeOrderType(OrderType.LIMIT)
                .takeLimitPrice(limitPrice);
        return intent;
    }

    /**
     * A strategy that answers its n-th update with what {@code onTick} gives and a risk reject with {@code onRiskReject}.
     * It counts the fills it hears of, and reports that reach it for an order after that order's final one.
     */
    private static final class ScriptedStrategy implements PythonStrategyAgent.PythonStrategyCallback {
        private final long processingTime;
        private final IntFunction<List<Intent>> onTick;
        Supplier<List<Intent>> onRiskReject = List::of;
        private int ticks;
        private final Set<Long> finished = new HashSet<>();
        long filled;
        int afterFinal;

        ScriptedStrategy(long processingTime, IntFunction<List<Intent>> onTick) {
            this.processingTime = processingTime;
            this.onTick = onTick;
        }

        @Override
        public List<Intent> onMarketData(Schema data) {
            return onTick.apply(ticks++);
        }

        @Override
        public List<Intent> onExecutionReport(OrderExecutionReport report) {
            ExecType type = report.decoder.execType();
            if (finished.contains(report.getClientOidCounter())) {
                afterFinal++;
            }
            if (type == ExecType.FILL || type == ExecType.CANCEL || type == ExecType.REJECT) {
                finished.add(report.getClientOidCounter());
            }
            if (type == ExecType.FILL || type == ExecType.PARTIAL_FILL) {
                filled += report.decoder.filledQty();
            }
            boolean riskReject =
                    type == ExecType.REJECT && report.decoder.rejectReason() == RejectReason.RISK_LIMIT_EXCEEDED;
            return riskReject ? onRiskReject.get() : List.of();
        }

        @Override
        public long simulateProcessingTime() {
            return processingTime;
        }

        @Override
        public void onInit(PositionView positionView, SecurityMaster securityMaster) {}
    }

    /** The real exchange, noting what reaches it and when, and how much it fills. */
    private static final class RecordingExchange implements SimulatedExchange {
        private final SimulatedExchange delegate;
        final List<Long> arrivals = new ArrayList<>();
        final List<Long> newOrders = new ArrayList<>();
        private final Set<Long> arrived = new HashSet<>();
        int cancels;
        int beforeTheirOrder;
        long filled;

        RecordingExchange(SimulatedExchange delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<OrderExecutionReport> submitOrder(Order order, long nowNanos) {
            arrivals.add(nowNanos);
            newOrders.add(order.getClientOidCounter());
            arrived.add(order.getClientOidCounter());
            return count(delegate.submitOrder(order, nowNanos));
        }

        @Override
        public List<OrderExecutionReport> cancelOrder(CancelOrder cancel, long nowNanos) {
            cancels++;
            if (!arrived.contains(cancel.getClientOidCounter())) {
                beforeTheirOrder++;
            }
            return count(delegate.cancelOrder(cancel, nowNanos));
        }

        @Override
        public List<OrderExecutionReport> modifyOrder(ModifyOrder modify, long nowNanos) {
            if (!arrived.contains(modify.getClientOidCounter())) {
                beforeTheirOrder++;
            }
            return count(delegate.modifyOrder(modify, nowNanos));
        }

        @Override
        public List<OrderExecutionReport> processDue(long nowNanos) {
            return count(delegate.processDue(nowNanos));
        }

        @Override
        public long nextDueNanos() {
            return delegate.nextDueNanos();
        }

        @Override
        public List<OrderExecutionReport> onMarketData(Schema data) {
            return count(delegate.onMarketData(data));
        }

        @Override
        public long simulateNetworkLatency() {
            return delegate.simulateNetworkLatency();
        }

        @Override
        public List<SchemaType> getSupportedSchemas() {
            return delegate.getSupportedSchemas();
        }

        private List<OrderExecutionReport> count(List<OrderExecutionReport> reports) {
            for (OrderExecutionReport report : reports) {
                ExecType type = report.decoder.execType();
                if (type == ExecType.FILL || type == ExecType.PARTIAL_FILL) {
                    filled += report.decoder.filledQty();
                }
            }
            return reports;
        }
    }

    private static final class Harness {
        final RecordingExchange exchange;
        private final PythonStrategyAgent strategy;
        private final LatencyConfig marketData;
        private final OmsBacktestAdapter adapter;
        private final PriceWriterAgent priceWriter;
        private final SequencedRingBuffer<Mbp10Schema> priceFeed;

        Harness(LatencyConfig network, ScriptedStrategy callback, RiskConfig risk) {
            this(network, network, callback, risk);
        }

        Harness(LatencyConfig marketData, LatencyConfig network, ScriptedStrategy callback, RiskConfig risk) {
            SecurityMaster securityMaster = mock(SecurityMaster.class);
            Listing listing = new Listing(
                    LISTING_ID,
                    new Exchange(EXCHANGE_ID, "KALSHI", "Test", "US", null),
                    new Security(SECURITY_ID, "SYM", null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                    "SYM",
                    "SYM");
            when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);
            when(securityMaster.getListing(LISTING_ID)).thenReturn(listing);
            when(securityMaster.getListingSpec(LISTING_ID)).thenReturn(new ListingSpec(LISTING_ID, 1, 0, 0, 1, 0));

            ExchangeProfileConfig profile = new ExchangeProfileConfig();
            profile.marketDataLatency = marketData;
            profile.networkLatency = network;
            ListingSimConfig listingConfig = new ListingSimConfig();
            listingConfig.listingId = LISTING_ID;
            listingConfig.profile = "default";
            BacktestConfig config = new BacktestConfig();
            config.listings = List.of(listingConfig);
            config.profiles = Map.of("default", profile);
            config.risk = risk;

            BacktestContext context = BacktestDriverFactory.buildContext(config);
            OrderManagementSystem oms = BacktestDriverFactory.buildOms(config, securityMaster, null, context);
            this.marketData = marketData;
            exchange = new RecordingExchange(profile.toSimulatedExchange(7));
            priceFeed = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
            strategy = PythonStrategyAgent.create(
                    0, oms.getPositionTracker().createPositionView(0), securityMaster, callback);
            adapter = new OmsBacktestAdapter(oms, context.clock());
            priceWriter =
                    new PriceWriterAgent(context.priceBuffer(), context.priceRegistry(), securityMaster, priceFeed);
        }

        void run(List<Schema> records) {
            BacktestDriver driver = driver(List.of(), null);
            driver.prepareRecords(records);
            driver.fullyExecute();
        }

        void runStreamed(List<MarketDataEntry> entries, S3Client s3) {
            BacktestDriver driver = driver(entries, s3);
            driver.prepareData();
            driver.fullyExecute();
        }

        private BacktestDriver driver(List<MarketDataEntry> entries, S3Client s3) {
            return new BacktestDriver(
                    entries,
                    strategy,
                    Map.of(EXCHANGE_ID, Map.of(SECURITY_ID, exchange)),
                    Map.of(exchange, MarketDataArrival.of(marketData, 7)),
                    adapter,
                    priceWriter,
                    priceFeed,
                    s3,
                    STREAM_BUCKET,
                    null,
                    false);
        }
    }
}
