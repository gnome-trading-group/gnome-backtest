package group.gnometrading.backtest.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.BacktestDriver;
import group.gnometrading.backtest.exchange.SimulatedExchange;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.logging.ConsoleLogger;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.position.DefaultPositionTracker;
import group.gnometrading.oms.position.SharedPositionBuffer;
import group.gnometrading.oms.risk.Configurable;
import group.gnometrading.oms.risk.MarketRiskPolicy;
import group.gnometrading.oms.risk.OrderRiskPolicy;
import group.gnometrading.oms.risk.PolicyFactory;
import group.gnometrading.oms.risk.RiskEngine;
import group.gnometrading.oms.risk.RiskPolicyType;
import group.gnometrading.oms.state.RingBufferOrderStateManager;
import group.gnometrading.sm.Listing;
import group.gnometrading.strategies.StrategyAgent;
import group.gnometrading.strings.ViewString;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.concurrent.SystemEpochNanoClock;
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
     */
    public static BacktestDriver create(
            BacktestConfig config,
            SecurityMaster securityMaster,
            OrderManagementSystem oms,
            StrategyAgent strategy,
            BacktestRecorder recorder,
            S3Client s3Client) {

        List<ResolvedListing> resolved = resolveListings(config, securityMaster);

        Map<Integer, Map<Integer, SimulatedExchange>> exchangeMap = buildExchangeMap(config, resolved);
        List<MarketDataEntry> entries = buildEntries(config, resolved);
        OmsBacktestAdapter adapter = new OmsBacktestAdapter(oms, recorder);

        return new BacktestDriver(entries, strategy, exchangeMap, adapter, s3Client, config.s3.bucket, recorder);
    }

    /**
     * Builds an OMS from a {@link RiskConfig} and SecurityMaster.
     * Exposed as a helper for callers that need to construct the OMS before the strategy.
     *
     * <p>Each entry in {@code risk.policies} is keyed by a {@link RiskPolicyType} name and mapped
     * to a parameter map. Order-time and market-time policies are split and loaded into the
     * global groups via {@link RiskEngine#withPolicies}.
     */
    public static OrderManagementSystem buildOms(RiskConfig risk, SecurityMaster securityMaster) {
        RiskEngine engine = buildRiskEngine(risk);
        SharedPositionBuffer sharedBuffer = new SharedPositionBuffer(64);
        return new OrderManagementSystem(
                new ConsoleLogger(new SystemEpochNanoClock()),
                new RingBufferOrderStateManager(),
                new DefaultPositionTracker(sharedBuffer),
                engine,
                securityMaster);
    }

    private static RiskEngine buildRiskEngine(RiskConfig risk) {
        if (risk == null || risk.policies.isEmpty()) {
            return new RiskEngine();
        }

        final PolicyFactory factory = new PolicyFactory();
        final ObjectMapper mapper = new ObjectMapper();
        final List<OrderRiskPolicy> orderPolicies = new ArrayList<>();
        final List<MarketRiskPolicy> marketPolicies = new ArrayList<>();

        for (Map.Entry<String, Map<String, Object>> entry : risk.policies.entrySet()) {
            final RiskPolicyType type = RiskPolicyType.valueOf(entry.getKey());
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
            map.computeIfAbsent(exchangeId, k -> new HashMap<>()).put(securityId, rl.profile.toSimulatedExchange());
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
