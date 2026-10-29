package group.gnometrading.backtest.config;

import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.BacktestDriver;
import group.gnometrading.backtest.driver.MarketDataArrival;
import group.gnometrading.backtest.driver.SimulatedClock;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.data.MarketDataApi;
import group.gnometrading.data.MissingData;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.logging.ConsoleLogger;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.ledger.LedgerSink;
import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.PriceWriter;
import group.gnometrading.oms.pnl.SharedPriceBuffer;
import group.gnometrading.oms.position.DefaultPositionTracker;
import group.gnometrading.oms.position.SharedPositionBuffer;
import group.gnometrading.oms.risk.ListingDirectory;
import group.gnometrading.oms.risk.PolicyFactory;
import group.gnometrading.oms.risk.RejectStreaks;
import group.gnometrading.oms.risk.RiskEngine;
import group.gnometrading.oms.state.PooledOrderStateManager;
import group.gnometrading.schemas.IntentEncoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.simulation.config.ExchangeSimulationConfig;
import group.gnometrading.simulation.config.SimulationResolver;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.simulation.latency.LatencySeeds;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.strategies.StrategyAgent;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;

public final class BacktestDriverFactory {

    private static final Logger LOGGER = Logger.getLogger(BacktestDriverFactory.class.getName());

    /** The book record the simulated exchanges match against. */
    private static final RecordType SIMULATED_BOOK_RECORD = RecordType.MBP_10;

    private BacktestDriverFactory() {}

    /**
     * Builds a fully configured BacktestDriver ready for {@code prepareData()}.
     *
     * <p>Listing IDs are resolved via SecurityMaster to exchange/security IDs and schema type.
     * The OMS and strategy must already be constructed so that the strategy's PositionView
     * is bound to the same OMS instance passed here.
     *
     * @param config          parsed backtest config
     * @param securityMaster  listing resolver
     * @param oms             pre-built OMS (shared with the strategy's PositionView)
     * @param strategy        pre-built strategy agent
     * @param recorder        optional recorder; null disables recording
     * @param s3Client        S3 client for market data loading
     * @param context         what the OMS was built with ({@link #buildContext})
     * @param marketDataApi   where collection says data is missing; null runs without knowing, as {@code ignore}
     */
    public static BacktestDriver create(
            BacktestConfig config,
            SecurityMaster securityMaster,
            OrderManagementSystem oms,
            StrategyAgent strategy,
            BacktestRecorder recorder,
            S3Client s3Client,
            BacktestContext context,
            MarketDataApi marketDataApi) {

        List<ResolvedListing> resolved = resolveListings(config, securityMaster);
        int[] tradedListingIds =
                resolved.stream().mapToInt(rl -> rl.listing.listingId()).toArray();
        StreamDirectory streams = StreamDirectory.of(
                securityMaster,
                tradedListingIds,
                config.streams.stream().mapToInt(stream -> stream.streamId).toArray());
        if (recorder != null) {
            recorder.configureStreams(streams);
        }

        Map<SimulatedExchange, MarketDataArrival> marketDataArrivals = new IdentityHashMap<>();
        Map<Integer, Map<Integer, SimulatedExchange>> exchangeMap =
                buildExchangeMap(config, resolved, securityMaster, marketDataArrivals);
        List<StreamEntry> entries = buildEntries(config, tradedListingIds, streams);
        // Its own feed rather than the strategy's buffer, written before the strategy sees each record: the OMS then
        // prices against exactly the books the strategy had seen, as it does live.
        SequencedRingBuffer<Mbp10Schema> priceFeed = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        PriceWriter priceWriter = new PriceWriter(context.priceBuffer(), context.priceRegistry(), streams, priceFeed);
        OmsBacktestAdapter adapter = new OmsBacktestAdapter(oms, priceWriter, context.clock(), recorder);

        String stage = System.getenv("STAGE");
        if (stage == null || stage.isEmpty()) {
            stage = "prod";
        }
        String bucket = "gnome-market-data-" + stage.toLowerCase();
        BacktestDriver driver = new BacktestDriver(
                entries,
                strategy,
                streams,
                exchangeMap,
                marketDataArrivals,
                adapter,
                priceWriter,
                priceFeed,
                s3Client,
                bucket,
                recorder,
                config.measureProcessingTime);
        if (marketDataApi != null) {
            applyMissing(
                    config,
                    driver,
                    marketDataApi.missing(streamIds(entries), nanos(config.startDate), nanos(config.endDate)));
        } else {
            LOGGER.warning("No market-data API given: the run can't know where its data is missing");
        }
        return driver;
    }

    /** Reports where data is missing, and marks it or refuses the run as the config says. */
    static void applyMissing(BacktestConfig config, BacktestDriver driver, List<MissingData> missing) {
        driver.reportMissingData(missing);
        long missingNs = missing.stream().mapToLong(MissingData::missingNs).sum();
        if (missingNs > 0) {
            LOGGER.warning(String.format(
                    "Market data missing: %.1f minutes across %d streams (%s)",
                    missingNs / 60e9,
                    missing.stream().filter(m -> m.missingNs() > 0).count(),
                    config.onMissing.name().toLowerCase()));
        }
        switch (config.onMissing) {
            case FAIL -> {
                if (missingNs > 0) {
                    throw new IllegalStateException("Market data is missing for this run, and on_missing is fail");
                }
            }
            case MARK -> missing.forEach(m -> driver.markMissing(m.streamId(), m.missing()));
            case IGNORE -> {}
        }
    }

    private static int[] streamIds(List<StreamEntry> entries) {
        return entries.stream().mapToInt(StreamEntry::getStreamId).distinct().toArray();
    }

    private static long nanos(LocalDateTime utc) {
        Instant instant = utc.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    /**
     * Builds what the OMS and driver share: a price buffer with a slot for every configured listing, and the simulated
     * clock. Pass the result to both {@link #buildOms} and {@link #create}.
     */
    public static BacktestContext buildContext(BacktestConfig config) {
        int capacity = Math.max(1, config.listings.size());
        SharedPriceBuffer buffer = new SharedPriceBuffer(capacity);
        PriceSlotRegistry registry = new PriceSlotRegistry(capacity);
        for (ListingSimConfig lsc : config.listings) {
            registry.register(lsc.listingId);
        }
        return new BacktestContext(buffer, registry, new SimulatedClock());
    }

    /**
     * Builds an OMS from a {@link RiskConfig} and SecurityMaster.
     * Exposed as a helper for callers that need to construct the OMS before the strategy.
     *
     * <p>Each entry in {@code risk.policies} is keyed by a {@link RiskPolicyType} name and mapped
     * to a parameter map. Order-time and market-time policies are split and loaded into the
     * global groups via {@link RiskEngine#withPolicies}.
     *
     * <p>The OMS the strategy trades through, under the config's risk policies. {@code registry} is only read when
     * {@code risk.from_registry} is set, and may be null otherwise.
     */
    public static OrderManagementSystem buildOms(
            BacktestConfig config,
            SecurityMaster securityMaster,
            RegistryConnection registry,
            BacktestContext context) {
        validateStrategyId(config.strategyId);
        RiskEngine engine = buildRiskEngine(config, securityMaster, registry, context);
        SharedPositionBuffer sharedBuffer = new SharedPositionBuffer(64);
        return new OrderManagementSystem(
                new ConsoleLogger(context.clock()),
                new PooledOrderStateManager(),
                new DefaultPositionTracker(sharedBuffer),
                engine,
                securityMaster,
                context.priceBuffer(),
                context.priceRegistry(),
                LedgerSink.NONE,
                context.clock());
    }

    private static void validateStrategyId(int strategyId) {
        if (strategyId < 0 || strategyId > IntentEncoder.strategyIdMaxValue()) {
            throw new IllegalArgumentException("strategy_id must be between 0 and " + IntentEncoder.strategyIdMaxValue()
                    + ", the range an intent carries, got " + strategyId);
        }
    }

    private static RiskEngine buildRiskEngine(
            BacktestConfig config,
            SecurityMaster securityMaster,
            RegistryConnection registry,
            BacktestContext context) {
        // Policies scoped to an exchange, or judging $1-payout contracts, need to know each listing's venue and kind.
        final List<Listing> listings = new ArrayList<>();
        for (ListingSimConfig lsc : config.listings) {
            final Listing listing = securityMaster.getListing(lsc.listingId);
            if (listing == null) {
                throw new IllegalArgumentException("Listing not found: " + lsc.listingId);
            }
            listings.add(listing);
        }
        final ListingDirectory directory = new ListingDirectory(listings);
        final RejectStreaks streaks = new RejectStreaks();
        final PolicyFactory factory =
                new PolicyFactory(context.priceBuffer(), context.priceRegistry(), directory, streaks);
        return RiskEngine.withScopedPolicies(
                BacktestRiskPolicies.load(config.risk, config.strategyId, registry, factory), directory, streaks);
    }

    /**
     * Resolves each listing's simulation: its exchange's registry parameters at the run's pinned time, then the
     * config's overrides for the exchange, then the listing's own.
     */
    private static List<ResolvedListing> resolveListings(BacktestConfig config, SecurityMaster securityMaster) {
        if (config.simulationAsOf == null) {
            config.simulationAsOf = Instant.now().toString();
        }
        SimulationResolver resolver = new SimulationResolver(securityMaster, config.simulationAsOf);
        List<ResolvedListing> result = new ArrayList<>();
        // A misspelt code would otherwise leave its exchange simulating with the registry's parameters, silently. A
        // real exchange the run doesn't trade is fine: one config's overrides serve every scenario run from it.
        Set<String> unknown = new TreeSet<>();
        for (String code : config.exchanges.keySet()) {
            if (securityMaster.getExchange(code) == null) {
                unknown.add(code);
            }
        }
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("exchanges overrides name unknown exchanges " + unknown);
        }
        for (ListingSimConfig lsc : config.listings) {
            Listing listing = securityMaster.getListing(lsc.listingId);
            if (listing == null) {
                throw new IllegalArgumentException("Listing not found: " + lsc.listingId);
            }
            ListingSpec spec = securityMaster.getListingSpec(lsc.listingId);
            if (spec == null) {
                throw new IllegalArgumentException("Listing spec not found for listing: " + lsc.listingId);
            }
            ExchangeSimulationConfig simulation = resolver.resolve(
                    listing, true, config.exchanges.get(listing.exchange().exchangeCode()), lsc.overrides);
            result.add(new ResolvedListing(listing, simulation));
        }
        return result;
    }

    /** Each listing's resolved simulation, as {@code listingId -> JSON}, for the run's metadata. */
    public static Map<Integer, String> resolvedSimulation(BacktestConfig config, SecurityMaster securityMaster) {
        Map<Integer, String> result = new LinkedHashMap<>();
        for (ResolvedListing rl : resolveListings(config, securityMaster)) {
            result.put(rl.listing.listingId(), rl.simulation.toJson());
        }
        return result;
    }

    /** Builds each listing's exchange, and records in {@code marketDataArrivals} when its market data reaches us. */
    private static Map<Integer, Map<Integer, SimulatedExchange>> buildExchangeMap(
            BacktestConfig config,
            List<ResolvedListing> resolved,
            SecurityMaster securityMaster,
            Map<SimulatedExchange, MarketDataArrival> marketDataArrivals) {
        Map<Integer, Map<Integer, SimulatedExchange>> map = new HashMap<>();
        for (ResolvedListing rl : resolved) {
            int exchangeId = rl.listing.exchange().exchangeId();
            int securityId = rl.listing.security().securityId();
            long listingSeed = LatencySeeds.derive(config.seed, rl.listing.listingId());
            SimulatedExchange exchange = rl.simulation.toSimulatedExchange(
                    rl.listing.listingId(),
                    listingSeed,
                    new SimulationResolver(securityMaster, config.simulationAsOf).registryFees());
            map.computeIfAbsent(exchangeId, k -> new HashMap<>()).put(securityId, exchange);
            marketDataArrivals.put(
                    exchange,
                    MarketDataArrival.of(
                            rl.simulation.marketDataLatency(),
                            LatencySeeds.derive(listingSeed, LatencySeeds.MARKET_DATA_STREAM)));
        }
        return map;
    }

    /**
     * One entry per minute of the run for each stream: the traded listings' books as MBP-10, which the simulated
     * exchanges match against whatever record their venue publishes, and every other stream as its config names.
     */
    static List<StreamEntry> buildEntries(BacktestConfig config, int[] tradedListingIds, StreamDirectory streams) {
        Map<Integer, RecordType> recordTypes = new LinkedHashMap<>();
        for (int listingId : tradedListingIds) {
            recordTypes.put(streams.streamId(listingId), SIMULATED_BOOK_RECORD);
        }
        for (StreamConfig stream : config.streams) {
            RecordType recordType = RecordType.forIdentifier(stream.recordType);
            RecordType replayed = recordTypes.putIfAbsent(stream.streamId, recordType);
            if (replayed != null && replayed != recordType) {
                throw new IllegalArgumentException("Stream %d is a traded listing's book, replayed as %s, not %s"
                        .formatted(stream.streamId, replayed.identifier(), recordType.identifier()));
            }
        }
        List<StreamEntry> entries = new ArrayList<>();
        recordTypes.forEach((streamId, recordType) -> {
            LocalDateTime current = config.startDate;
            while (current.isBefore(config.endDate)) {
                entries.add(new StreamEntry(streamId, recordType, current));
                current = current.plusMinutes(1);
            }
        });
        return entries;
    }

    private record ResolvedListing(Listing listing, ExchangeSimulationConfig simulation) {}
}
