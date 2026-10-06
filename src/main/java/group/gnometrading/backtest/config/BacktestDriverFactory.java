package group.gnometrading.backtest.config;

import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.BacktestDriver;
import group.gnometrading.backtest.driver.MarketDataArrival;
import group.gnometrading.backtest.driver.SimulatedClock;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.logging.ConsoleLogger;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.ledger.LedgerSink;
import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.PriceWriterAgent;
import group.gnometrading.oms.pnl.SharedPriceBuffer;
import group.gnometrading.oms.position.DefaultPositionTracker;
import group.gnometrading.oms.position.SharedPositionBuffer;
import group.gnometrading.oms.risk.PolicyFactory;
import group.gnometrading.oms.risk.RiskEngine;
import group.gnometrading.oms.state.PooledOrderStateManager;
import group.gnometrading.schemas.IntentEncoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.simulation.config.ExchangeProfileConfig;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.simulation.latency.LatencySeeds;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.strategies.StrategyAgent;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.s3.S3Client;

public final class BacktestDriverFactory {

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
     */
    public static BacktestDriver create(
            BacktestConfig config,
            SecurityMaster securityMaster,
            OrderManagementSystem oms,
            StrategyAgent strategy,
            BacktestRecorder recorder,
            S3Client s3Client,
            BacktestContext context) {

        List<ResolvedListing> resolved = resolveListings(config, securityMaster);

        Map<SimulatedExchange, MarketDataArrival> marketDataArrivals = new IdentityHashMap<>();
        Map<Integer, Map<Integer, SimulatedExchange>> exchangeMap =
                buildExchangeMap(config, resolved, marketDataArrivals);
        List<MarketDataEntry> entries = buildEntries(config, resolved);
        OmsBacktestAdapter adapter = new OmsBacktestAdapter(oms, context.clock(), recorder);
        // Its own feed rather than the strategy's buffer: live it reads market data as it arrives, even while the
        // strategy is busy.
        SequencedRingBuffer<Mbp10Schema> priceFeed = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        PriceWriterAgent priceWriter =
                new PriceWriterAgent(context.priceBuffer(), context.priceRegistry(), securityMaster, priceFeed);

        String stage = System.getenv("STAGE");
        if (stage == null || stage.isEmpty()) {
            stage = "prod";
        }
        String bucket = "gnome-market-data-" + stage.toLowerCase();
        return new BacktestDriver(
                entries,
                strategy,
                exchangeMap,
                marketDataArrivals,
                adapter,
                priceWriter,
                priceFeed,
                s3Client,
                bucket,
                recorder,
                config.measureProcessingTime);
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
        RiskEngine engine = buildRiskEngine(config, registry, context);
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
            BacktestConfig config, RegistryConnection registry, BacktestContext context) {
        final PolicyFactory factory = new PolicyFactory(context.priceBuffer(), context.priceRegistry());
        return RiskEngine.withScopedPolicies(
                BacktestRiskPolicies.load(config.risk, config.strategyId, registry, factory));
    }

    private static List<ResolvedListing> resolveListings(BacktestConfig config, SecurityMaster securityMaster) {
        List<ResolvedListing> result = new ArrayList<>();
        for (ListingSimConfig lsc : config.listings) {
            Listing listing = securityMaster.getListing(lsc.listingId);
            if (listing == null) {
                throw new IllegalArgumentException("Listing not found: " + lsc.listingId);
            }
            ListingSpec spec = securityMaster.getListingSpec(lsc.listingId);
            if (spec == null) {
                throw new IllegalArgumentException("Listing spec not found for listing: " + lsc.listingId);
            }
            ExchangeProfileConfig profile = config.profiles.get(lsc.profile);
            if (profile == null) {
                throw new IllegalArgumentException(
                        "Profile '" + lsc.profile + "' not found for listing " + lsc.listingId);
            }
            result.add(new ResolvedListing(listing, profile));
        }
        return result;
    }

    /** Builds each listing's exchange, and records in {@code marketDataArrivals} when its market data reaches us. */
    private static Map<Integer, Map<Integer, SimulatedExchange>> buildExchangeMap(
            BacktestConfig config,
            List<ResolvedListing> resolved,
            Map<SimulatedExchange, MarketDataArrival> marketDataArrivals) {
        Map<Integer, Map<Integer, SimulatedExchange>> map = new HashMap<>();
        for (ResolvedListing rl : resolved) {
            int exchangeId = rl.listing.exchange().exchangeId();
            int securityId = rl.listing.security().securityId();
            long listingSeed = LatencySeeds.derive(config.seed, rl.listing.listingId());
            SimulatedExchange exchange = rl.profile.toSimulatedExchange(listingSeed);
            map.computeIfAbsent(exchangeId, k -> new HashMap<>()).put(securityId, exchange);
            marketDataArrivals.put(
                    exchange,
                    MarketDataArrival.of(
                            rl.profile.marketDataLatency,
                            LatencySeeds.derive(listingSeed, LatencySeeds.MARKET_DATA_STREAM)));
        }
        return map;
    }

    private static List<MarketDataEntry> buildEntries(BacktestConfig config, List<ResolvedListing> resolved) {
        List<MarketDataEntry> entries = new ArrayList<>();
        for (ResolvedListing rl : resolved) {
            int securityId = rl.listing.security().securityId();
            int exchangeId = rl.listing.exchange().exchangeId();
            var schemaType = rl.listing.exchange().schemaType();
            LocalDateTime current = config.startDate;
            while (current.isBefore(config.endDate)) {
                entries.add(new MarketDataEntry(
                        securityId, exchangeId, schemaType, current, MarketDataEntry.EntryType.AGGREGATED));
                current = current.plusMinutes(1);
            }
        }
        return entries;
    }

    private record ResolvedListing(Listing listing, ExchangeProfileConfig profile) {}
}
