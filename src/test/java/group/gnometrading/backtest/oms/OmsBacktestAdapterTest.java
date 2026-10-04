package group.gnometrading.backtest.oms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;

import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.logging.NullLogger;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.SharedPriceBuffer;
import group.gnometrading.oms.position.DefaultPositionTracker;
import group.gnometrading.oms.position.SharedPositionBuffer;
import group.gnometrading.oms.risk.RiskEngine;
import group.gnometrading.oms.state.RingBufferOrderStateManager;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
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

    private OmsBacktestAdapter adapter;

    @BeforeEach
    void setUp() {
        OrderManagementSystem oms = new OrderManagementSystem(
                new NullLogger(),
                new RingBufferOrderStateManager(64),
                new DefaultPositionTracker(new SharedPositionBuffer(16)),
                new RiskEngine(),
                securityMaster,
                new SharedPriceBuffer(1),
                new PriceSlotRegistry(1));
        adapter = new OmsBacktestAdapter(oms);

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
