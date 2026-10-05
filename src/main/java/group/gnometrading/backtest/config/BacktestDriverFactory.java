package group.gnometrading.backtest.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.BacktestDriver;
import group.gnometrading.backtest.driver.SimulatedClock;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.logging.ConsoleLogger;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.PriceWriterAgent;
import group.gnometrading.oms.pnl.SharedPriceBuffer;
import group.gnometrading.oms.position.DefaultPositionTracker;
import group.gnometrading.oms.position.SharedPositionBuffer;
import group.gnometrading.oms.risk.Configurable;
import group.gnometrading.oms.risk.MarketRiskPolicy;
import group.gnometrading.oms.risk.OrderRiskPolicy;
import group.gnometrading.oms.risk.PolicyFactory;
import group.gnometrading.oms.risk.RiskEngine;
import group.gnometrading.oms.risk.RiskPolicyType;
import group.gnometrading.oms.state.PooledOrderStateManager;
import group.gnometrading.simulation.config.ExchangeProfileConfig;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.simulation.latency.LatencySeeds;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.strategies.StrategyAgent;
import group.gnometrading.strings.ViewString;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
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

        Map<Integer, Map<Integer, SimulatedExchange>> exchangeMap = buildExchangeMap(config, resolved);
        List<MarketDataEntry> entries = buildEntries(config, resolved);
        OmsBacktestAdapter adapter = new OmsBacktestAdapter(oms, context.clock(), recorder);
        PriceWriterAgent priceWriter = new PriceWriterAgent(
                context.priceBuffer(), context.priceRegistry(), securityMaster, strategy.getMarketDataBuffer());

        String stage = System.getenv("STAGE");
        if (stage == null || stage.isEmpty()) {
            stage = "prod";
        }
        String bucket = "gnome-market-data-" + stage.toLowerCase();
        return new BacktestDriver(
                entries,
                strategy,
                exchangeMap,
                adapter,
                priceWriter,
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
     */
    public static OrderManagementSystem buildOms(
            RiskConfig risk, SecurityMaster securityMaster, BacktestContext context) {
        RiskEngine engine = buildRiskEngine(risk, context);
        SharedPositionBuffer sharedBuffer = new SharedPositionBuffer(64);
        return new OrderManagementSystem(
                new ConsoleLogger(context.clock()),
                new PooledOrderStateManager(),
                new DefaultPositionTracker(sharedBuffer),
                engine,
                securityMaster,
                context.priceBuffer(),
                context.priceRegistry(),
                context.clock());
    }

    private static RiskEngine buildRiskEngine(RiskConfig risk, BacktestContext context) {
        if (risk == null || risk.policies.isEmpty()) {
            return new RiskEngine();
        }

        final PolicyFactory factory = new PolicyFactory(context.priceBuffer(), context.priceRegistry());
        final ObjectMapper mapper = new ObjectMapper();
        final List<OrderRiskPolicy> orderPolicies = new ArrayList<>();
        final List<MarketRiskPolicy> marketPolicies = new ArrayList<>();

        for (Map.Entry<String, Map<String, Object>> entry : risk.policies.entrySet()) {
            final RiskPolicyType type = RiskPolicyType.valueOf(entry.getKey());
            if (type.category() == RiskPolicyType.Category.KILL) {
                throw new IllegalArgumentException(
                        type + " stops all trading and has no meaning in a backtest; remove it from risk.policies");
            }
            final Map<String, Object> params = entry.getValue() != null ? entry.getValue() : Map.of();
            final Configurable policy = factory.create(type);
            try {
                policy.reconfigure(new ViewString(mapper.writeValueAsString(params)));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to serialize params for policy " + type, e);
            }
            switch (type.category()) {
                case ORDER -> orderPolicies.add((OrderRiskPolicy) policy);
                case MARKET -> marketPolicies.add((MarketRiskPolicy) policy);
                case KILL -> throw new IllegalStateException("unreachable: kill switches are rejected above");
            }
        }

        return RiskEngine.withPolicies(
                orderPolicies.toArray(new OrderRiskPolicy[0]), marketPolicies.toArray(new MarketRiskPolicy[0]));
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

    private static Map<Integer, Map<Integer, SimulatedExchange>> buildExchangeMap(
            BacktestConfig config, List<ResolvedListing> resolved) {
        Map<Integer, Map<Integer, SimulatedExchange>> map = new HashMap<>();
        for (ResolvedListing rl : resolved) {
            int exchangeId = rl.listing.exchange().exchangeId();
            int securityId = rl.listing.security().securityId();
            long listingSeed = LatencySeeds.derive(config.seed, rl.listing.listingId());
            map.computeIfAbsent(exchangeId, k -> new HashMap<>())
                    .put(securityId, rl.profile.toSimulatedExchange(listingSeed));
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
