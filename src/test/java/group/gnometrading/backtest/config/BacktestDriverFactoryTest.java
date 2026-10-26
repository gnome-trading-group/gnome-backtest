package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.collections.IntToIntHashMap;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.md.Stream;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.simulation.config.LatencyConfig;
import group.gnometrading.simulation.config.QueueModelConfig;
import group.gnometrading.simulation.config.SimulationOverrides;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.Security;
import group.gnometrading.sm.SimulationParameters;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BacktestDriverFactoryTest {

    private static final int EXCHANGE_ID = 1;
    private static final int SECURITY_ID = 42;
    private static final int LISTING_ID = 100;
    private static final int STRATEGY_ID = 12;
    private static final int OTHER_STRATEGY = 13;
    private static final long DOLLAR = Statics.PRICE_SCALING_FACTOR;
    private static final long UNIT = Statics.SIZE_SCALING_FACTOR;

    @Mock
    private SecurityMaster securityMaster;

    private BacktestConfig config;

    @BeforeEach
    void setUp() {
        ListingSimConfig lsc = new ListingSimConfig();
        lsc.listingId = LISTING_ID;
        config = new BacktestConfig();
        config.listings = List.of(lsc);
        config.risk = new RiskConfig();

        Listing listing = new Listing(
                LISTING_ID,
                new Exchange(EXCHANGE_ID, "POLYMARKET_INTL", "Test", "US"),
                new Security(SECURITY_ID, "SYM", null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                "SYM",
                "SYM");
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);
        when(securityMaster.getListing(LISTING_ID)).thenReturn(listing);
        when(securityMaster.getListingSpec(LISTING_ID)).thenReturn(new ListingSpec(LISTING_ID, 1, 0, DOLLAR, 1, 0));
        when(securityMaster.getSimulationParameters(eq(EXCHANGE_ID), anyString()))
                .thenReturn(new SimulationParameters(1, EXCHANGE_ID, "1970-01-01T00:00:00.000Z", RECORDED));
    }

    private static final String RECORDED =
            """
            {"network_latency": {"type": "static", "latency_nanos": 50000000},
             "order_processing_latency": {"type": "static", "latency_nanos": 5000000},
             "market_data_latency": {"type": "recorded", "fallback": {"type": "static", "latency_nanos": 50000000}},
             "queue_model": {"type": "risk_averse"},
             "self_trade_prevention": "CANCEL_INCOMING"}""";

    /** One traded listing's book stream (7) and a vendor stream (9) the strategy also reads. */
    private StreamDirectory streamsWithAVendorFeed() {
        StreamDirectory streams = new StreamDirectory();
        streams.add(
                new Stream(7, 1, "POLYMARKET_INTL", "LISTING", LISTING_ID, null, "book", "us-east-1", 0, 0L, 0L, true),
                securityMaster.getListing(LISTING_ID));
        streams.add(new Stream(9, 2, "PANDASCORE", "MATCH", 0, "m-1", "book", "eu-west-1", 0, 0L, 0L, true), null);
        return streams;
    }

    private static StreamConfig streamConfig(int streamId, RecordType recordType) {
        StreamConfig stream = new StreamConfig();
        stream.streamId = streamId;
        stream.recordType = recordType.identifier();
        return stream;
    }

    @Test
    void tradedBooksReplayAsTheRecordTheSimulatorMatchesAndOtherStreamsAsConfigured() {
        config.startDate = LocalDateTime.of(2026, 1, 1, 0, 0);
        config.endDate = LocalDateTime.of(2026, 1, 1, 0, 2);
        config.streams = List.of(streamConfig(9, RecordType.TRADES));

        List<StreamEntry> entries =
                BacktestDriverFactory.buildEntries(config, new int[] {LISTING_ID}, streamsWithAVendorFeed());

        assertEquals(
                List.of(
                        "7/book/mbp-10/2026/01/01/00/00.zst",
                        "7/book/mbp-10/2026/01/01/00/01.zst",
                        "9/book/trades/2026/01/01/00/00.zst",
                        "9/book/trades/2026/01/01/00/01.zst"),
                entries.stream().map(StreamEntry::getKey).toList());
    }

    @Test
    void aTradedBookConfiguredAsAnotherRecordIsRefused() {
        config.startDate = LocalDateTime.of(2026, 1, 1, 0, 0);
        config.endDate = LocalDateTime.of(2026, 1, 1, 0, 1);
        config.streams = List.of(streamConfig(7, RecordType.MBO));

        assertThrows(
                IllegalArgumentException.class,
                () -> BacktestDriverFactory.buildEntries(config, new int[] {LISTING_ID}, streamsWithAVendorFeed()));
    }

    @Test
    void listingsSimulateWithTheirExchangesRecordedParametersUnderTheRunsOverrides() {
        SimulationOverrides exchange = new SimulationOverrides();
        exchange.queueModel = new QueueModelConfig.Optimistic();
        config.exchanges = Map.of("POLYMARKET_INTL", exchange);
        when(securityMaster.getExchange("POLYMARKET_INTL"))
                .thenReturn(new Exchange(EXCHANGE_ID, "POLYMARKET_INTL", "Test", "US"));
        SimulationOverrides listing = new SimulationOverrides();
        listing.networkLatency = new LatencyConfig.Static();
        config.listings.get(0).overrides = listing;

        String resolved =
                BacktestDriverFactory.resolvedSimulation(config, securityMaster).get(LISTING_ID);

        assertTrue(resolved.contains("\"queue_model\":{\"type\":\"optimistic\"}"), resolved);
        assertTrue(resolved.contains("\"network_latency\":{\"type\":\"static\",\"latency_nanos\":0}"), resolved);
        assertTrue(
                resolved.contains("\"order_processing_latency\":{\"type\":\"static\",\"latency_nanos\":5000000}"),
                resolved);
        assertTrue(resolved.contains("\"fee_model\":{\"type\":\"registry\"}"), resolved);
    }

    @Test
    void theRunPinsTheTimeItReadTheRegistryAt() {
        BacktestDriverFactory.resolvedSimulation(config, securityMaster);
        String pinned = config.simulationAsOf;
        assertDoesNotThrow(() -> java.time.Instant.parse(pinned));
        BacktestDriverFactory.resolvedSimulation(config, securityMaster);
        assertEquals(pinned, config.simulationAsOf);
    }

    @Test
    void anOverrideForAnUnknownExchangeIsRefused() {
        config.exchanges = Map.of("POLYMARKET", new SimulationOverrides());
        IllegalArgumentException refused = assertThrows(
                IllegalArgumentException.class, () -> BacktestDriverFactory.resolvedSimulation(config, securityMaster));
        assertEquals("exchanges overrides name unknown exchanges [POLYMARKET]", refused.getMessage());
    }

    @Test
    void anOverrideForARealExchangeTheRunDoesNotTradeIsLeftAlone() {
        when(securityMaster.getExchange("KALSHI")).thenReturn(new Exchange(9, "KALSHI", "Kalshi", "us-east-1"));
        config.exchanges = Map.of("KALSHI", new SimulationOverrides());
        assertDoesNotThrow(() -> BacktestDriverFactory.resolvedSimulation(config, securityMaster));
    }

    @Test
    void buildPricesRegistersEveryConfiguredListing() {
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        assertNotEquals(IntToIntHashMap.MISSING, context.priceRegistry().getSlot(LISTING_ID));
    }

    @Test
    void marketOrderIsValuedFromTheBacktestPriceBuffer() throws Exception {
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        OmsBacktestAdapter adapter = new OmsBacktestAdapter(
                BacktestDriverFactory.buildOms(config, securityMaster, null, context), context.clock());
        context.priceBuffer().writeQuote(context.priceRegistry().getSlot(LISTING_ID), DOLLAR / 10, DOLLAR / 4);

        // 4 at the $0.25 ask meets the $1 minimum; without the buffer the OMS had no price and rejected it.
        assertEquals(1, orderCount(adapter.processIntents(0, List.of(marketIntent(Side.Bid, 4 * UNIT)))));
        // 4 at the $0.10 bid does not.
        assertEquals(0, orderCount(adapter.processIntents(1, List.of(marketIntent(Side.Ask, 4 * UNIT)))));
    }

    @Test
    void totalPnlLossPolicyBuildsWithTheBacktestPriceBuffer() {
        RiskConfig.PolicyConfig policy = new RiskConfig.PolicyConfig();
        policy.type = "MAX_TOTAL_PNL_LOSS";
        policy.params = Map.of("maxLoss", 100 * DOLLAR);
        config.risk.policies.add(policy);
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        assertDoesNotThrow(() -> BacktestDriverFactory.buildOms(config, securityMaster, null, context));
    }

    @Test
    void scopedPolicyAppliesOnlyToItsStrategy() throws Exception {
        config.strategyId = STRATEGY_ID;
        RiskConfig.PolicyConfig scoped = new RiskConfig.PolicyConfig();
        scoped.type = "MAX_ORDER_SIZE";
        scoped.strategyId = STRATEGY_ID;
        scoped.params = Map.of("maxOrderSize", 5 * UNIT);
        config.risk.policies.add(scoped);
        OmsBacktestAdapter adapter = quotedAdapter(null);

        assertEquals(0, orderCount(adapter.processIntents(0, List.of(marketIntent(STRATEGY_ID, Side.Bid, 6 * UNIT)))));
        assertEquals(
                1, orderCount(adapter.processIntents(1, List.of(marketIntent(OTHER_STRATEGY, Side.Bid, 6 * UNIT)))));
    }

    @Test
    void registryPoliciesApplyWithoutKillSwitchesOrOtherSessions() throws Exception {
        config.strategyId = STRATEGY_ID;
        config.risk.fromRegistry = true;
        RegistryConnection registry = mock(RegistryConnection.class);
        when(registry.get(argThat(path -> path.toString().contains("forStrategy=" + STRATEGY_ID + "&forSession=none"))))
                .thenAnswer(call -> ByteBuffer.wrap(("["
                                + policyRow(1, "MAX_ORDER_SIZE", null, STRATEGY_ID, 5 * UNIT, true) + ","
                                + policyRow(2, "KILL_SWITCH", null, STRATEGY_ID, 0, true) + ","
                                + policyRow(3, "MAX_ORDER_SIZE", null, 0, 0, false) + ","
                                + policyRow(4, "MAX_ORDER_SIZE", "\"abc\"", 0, 0, true) + "]")
                        .getBytes(StandardCharsets.UTF_8)));
        OmsBacktestAdapter adapter = quotedAdapter(registry);

        assertEquals(1, orderCount(adapter.processIntents(0, List.of(marketIntent(STRATEGY_ID, Side.Bid, 4 * UNIT)))));
        assertEquals(0, orderCount(adapter.processIntents(1, List.of(marketIntent(STRATEGY_ID, Side.Bid, 6 * UNIT)))));
    }

    @Test
    void registryPoliciesNeedAStrategyId() {
        config.risk.fromRegistry = true;
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        RegistryConnection registry = mock(RegistryConnection.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> BacktestDriverFactory.buildOms(config, securityMaster, registry, context));
    }

    @Test
    void strategyIdMustFitAnIntent() {
        config.strategyId = 1 << 16;
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        assertThrows(
                IllegalArgumentException.class,
                () -> BacktestDriverFactory.buildOms(config, securityMaster, null, context));
    }

    private OmsBacktestAdapter quotedAdapter(RegistryConnection registry) {
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        OmsBacktestAdapter adapter = new OmsBacktestAdapter(
                BacktestDriverFactory.buildOms(config, securityMaster, registry, context), context.clock());
        context.priceBuffer().writeQuote(context.priceRegistry().getSlot(LISTING_ID), DOLLAR / 10, DOLLAR / 4);
        return adapter;
    }

    private static String policyRow(
            int id, String type, String session, int strategyId, long maxOrderSize, boolean enabled) {
        return "{\"policy_id\": " + id + ", \"policy_type\": \"" + type + "\", \"session_id\": " + session
                + ", \"strategy_id\": " + strategyId + ", \"listing_id\": null, \"parameters\": {\"maxOrderSize\": "
                + maxOrderSize + "}, \"enabled\": " + enabled + "}";
    }

    private static long orderCount(List<LocalMessage> messages) {
        return messages.stream()
                .filter(m -> m instanceof LocalMessage.OrderMessage)
                .count();
    }

    private static Intent marketIntent(Side side, long size) {
        return marketIntent(0, side, size);
    }

    private static Intent marketIntent(int strategyId, Side side, long size) {
        Intent intent = new Intent();
        intent.encoder
                .strategyId(strategyId)
                .exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .bidPrice(IntentDecoder.bidPriceNullValue())
                .bidSize(IntentDecoder.bidSizeNullValue())
                .askPrice(IntentDecoder.askPriceNullValue())
                .askSize(IntentDecoder.askSizeNullValue())
                .takeSize(size)
                .takeSide(side)
                .takeOrderType(OrderType.MARKET)
                .takeLimitPrice(IntentDecoder.takeLimitPriceNullValue());
        return intent;
    }
}
