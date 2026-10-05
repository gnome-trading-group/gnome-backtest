package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.when;

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
                BacktestDriverFactory.buildOms(config.risk, securityMaster, context), context.clock());
        context.priceBuffer().writeQuote(context.priceRegistry().getSlot(LISTING_ID), DOLLAR / 10, DOLLAR / 4);

        // 4 at the $0.25 ask meets the $1 minimum; without the buffer the OMS had no price and rejected it.
        assertEquals(1, orderCount(adapter.processIntents(0, List.of(marketIntent(Side.Bid, 4 * UNIT)))));
        // 4 at the $0.10 bid does not.
        assertEquals(0, orderCount(adapter.processIntents(1, List.of(marketIntent(Side.Ask, 4 * UNIT)))));
    }

    @Test
    void totalPnlLossPolicyBuildsWithTheBacktestPriceBuffer() {
        config.risk.policies.put("MAX_TOTAL_PNL_LOSS", Map.of("maxLoss", 100 * DOLLAR));
        BacktestContext context = BacktestDriverFactory.buildContext(config);
        assertDoesNotThrow(() -> BacktestDriverFactory.buildOms(config.risk, securityMaster, context));
    }

    private static long orderCount(List<LocalMessage> messages) {
        return messages.stream()
                .filter(m -> m instanceof LocalMessage.OrderMessage)
                .count();
    }

    private static Intent marketIntent(Side side, long size) {
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
                .takeOrderType(OrderType.MARKET)
                .takeLimitPrice(IntentDecoder.takeLimitPriceNullValue());
        return intent;
    }
}
