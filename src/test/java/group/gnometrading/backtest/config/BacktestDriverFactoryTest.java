package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.collections.IntToIntHashMap;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.Statics;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.Security;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
                new Exchange(EXCHANGE_ID, "POLYMARKET_INTL", "Test", "US", null),
                new Security(SECURITY_ID, "SYM", null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                "SYM",
                "SYM");
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);
        when(securityMaster.getListing(LISTING_ID)).thenReturn(listing);
        when(securityMaster.getListingSpec(LISTING_ID)).thenReturn(new ListingSpec(LISTING_ID, 1, 0, DOLLAR, 1, 0));
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
