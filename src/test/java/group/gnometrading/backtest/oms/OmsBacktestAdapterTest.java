package group.gnometrading.backtest.oms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.when;

import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.driver.SimulatedClock;
import group.gnometrading.logging.NullLogger;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.SharedPriceBuffer;
import group.gnometrading.oms.position.DefaultPositionTracker;
import group.gnometrading.oms.position.SharedPositionBuffer;
import group.gnometrading.oms.risk.RiskEngine;
import group.gnometrading.oms.state.PooledOrderStateManager;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OmsBacktestAdapterTest {

    private static final int EXCHANGE_ID = 1;
    private static final int SECURITY_ID = 42;
    private static final int LISTING_ID = 100;
    private static final int STRATEGY_ID = 7;

    @Mock
    private SecurityMaster securityMaster;

    private final SimulatedClock clock = new SimulatedClock();
    private OmsBacktestAdapter adapter;

    @BeforeEach
    void setUp() {
        OrderManagementSystem oms = new OrderManagementSystem(
                new NullLogger(),
                new PooledOrderStateManager(64),
                new DefaultPositionTracker(new SharedPositionBuffer(16)),
                new RiskEngine(),
                securityMaster,
                new SharedPriceBuffer(1),
                new PriceSlotRegistry(1),
                clock);
        adapter = new OmsBacktestAdapter(oms, clock);

        Listing listing = new Listing(
                LISTING_ID,
                new Exchange(EXCHANGE_ID, "KALSHI", "Test", "US", null),
                new Security(SECURITY_ID, "SYM", null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                "SYM",
                "SYM");
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);
        when(securityMaster.getListing(LISTING_ID)).thenReturn(listing);
    }

    @Test
    void outboundOrdersAreStampedWithSimulatedTime() throws Exception {
        long simulatedTime = 1_700_000_000_123_456_789L;

        List<LocalMessage> messages = adapter.processIntents(simulatedTime, List.of(bidIntent(100L, 10L)));

        assertEquals(1, messages.size());
        LocalMessage.OrderMessage orderMessage = assertInstanceOf(LocalMessage.OrderMessage.class, messages.get(0));
        assertEquals(simulatedTime, orderMessage.order().decoder.timestampSend());
    }

    @Test
    void manyRejectedIntentsInOneStepDoNotHang() {
        // Every order is below the minimum size, so each two-sided intent yields two synthetic rejects.
        when(securityMaster.getListingSpec(LISTING_ID)).thenReturn(new ListingSpec(LISTING_ID, 1, 0, 0, 1, 1_000_000));
        List<Intent> intents = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            intents.add(twoSidedIntent(100L, 110L, 10L));
        }

        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> adapter.processIntents(1L, intents));

        assertEquals(400, adapter.getStrategyExecReports().size());
        // Synthetic rejects carry the simulated time they were made at.
        assertEquals(1L, adapter.getStrategyExecReports().get(0).decoder.timestampRecv());
    }

    @Test
    void manyOrderActionsInOneStepDoNotHang() {
        OrderManagementSystem oms = new OrderManagementSystem(
                new NullLogger(),
                new PooledOrderStateManager(512),
                new DefaultPositionTracker(new SharedPositionBuffer(256)),
                new RiskEngine(),
                securityMaster,
                new SharedPriceBuffer(1),
                new PriceSlotRegistry(1),
                clock);
        OmsBacktestAdapter wide = new OmsBacktestAdapter(oms, clock);
        List<Intent> intents = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            int securityId = 1_000 + i;
            Listing listing = new Listing(
                    LISTING_ID + 1 + i,
                    new Exchange(EXCHANGE_ID, "KALSHI", "Test", "US", null),
                    new Security(
                            securityId, "S" + i, null, null, null, null, null, null, false, false, 0L, 0L, true, 0),
                    "S" + i,
                    "S" + i);
            when(securityMaster.getListing(EXCHANGE_ID, securityId)).thenReturn(listing);
            Intent intent = twoSidedIntent(100L, 110L, 10L);
            intent.encoder.securityId(securityId);
            intents.add(intent);
        }

        List<LocalMessage> messages =
                assertTimeoutPreemptively(Duration.ofSeconds(10), () -> wide.processIntents(1L, intents));

        assertEquals(300, messages.size());
    }

    private static Intent twoSidedIntent(long bidPrice, long askPrice, long size) {
        Intent intent = new Intent();
        intent.encoder
                .strategyId(STRATEGY_ID)
                .exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .bidPrice(bidPrice)
                .bidSize(size)
                .askPrice(askPrice)
                .askSize(size)
                .takeSize(IntentDecoder.takeSizeNullValue());
        return intent;
    }

    private static Intent bidIntent(long price, long size) {
        Intent intent = new Intent();
        intent.encoder
                .strategyId(STRATEGY_ID)
                .exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .bidPrice(price)
                .bidSize(size)
                .askPrice(IntentDecoder.askPriceNullValue())
                .askSize(IntentDecoder.askSizeNullValue())
                .takeSize(IntentDecoder.takeSizeNullValue());
        return intent;
    }
}
