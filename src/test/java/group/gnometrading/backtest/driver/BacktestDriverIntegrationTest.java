package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.backtest.config.BacktestConfig;
import group.gnometrading.backtest.config.BacktestDriverFactory;
import group.gnometrading.backtest.config.ExchangeProfileConfig;
import group.gnometrading.backtest.config.ListingSimConfig;
import group.gnometrading.backtest.config.RiskConfig;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Side;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

class BacktestDriverIntegrationTest {

    private static final String REGISTRY_HOST = "n5dxpwnij0.execute-api.us-east-1.amazonaws.com";
    private static final int LISTING_ID = 1;

    @Test
    void momentumStrategyRunsEndToEnd() throws Exception {
        String apiKey = System.getenv("GNOME_REGISTRY_API_KEY");
        assumeTrue(apiKey != null && !apiKey.isBlank(), "GNOME_REGISTRY_API_KEY not set — skipping integration test");

        SecurityMaster securityMaster = new SecurityMaster(new RegistryConnection(REGISTRY_HOST, apiKey));

        BacktestConfig config = new BacktestConfig();
        config.startDate = LocalDateTime.of(2026, 1, 23, 10, 30);
        config.endDate = LocalDateTime.of(2026, 1, 23, 11, 0);
        config.risk = new RiskConfig();
        config.record = true;
        config.recordDepth = 1;

        ListingSimConfig lsc = new ListingSimConfig();
        lsc.listingId = LISTING_ID;
        lsc.profile = "default";
        config.listings = List.of(lsc);
        config.profiles = Map.of("default", new ExchangeProfileConfig());

        OrderManagementSystem oms = BacktestDriverFactory.buildOms(config.risk, securityMaster);
        PositionView positionView = oms.getPositionTracker().createPositionView(0);

        MomentumCallback callback = new MomentumCallback();
        PythonStrategyAgent strategy = PythonStrategyAgent.create(positionView, callback);

        BacktestRecorder recorder = new BacktestRecorder(config.recordDepth);
        S3Client s3Client = S3Client.create();

        BacktestDriver driver = BacktestDriverFactory.create(config, securityMaster, oms, strategy, recorder, s3Client);
        driver.prepareData();
        driver.fullyExecute();

        assertTrue(
                driver.getEventsProcessed() > 0,
                "No events processed — market data may be missing for this date range");
        assertTrue(callback.signalCount > 0, "Strategy never produced a signal — check warmup or data quality");
    }

    /**
     * Simple momentum taker: after a warmup period, buys when mid is trending up and sells
     * when trending down, using aggressive market IOC orders.
     */
    static class MomentumCallback implements PythonStrategyAgent.PythonStrategyCallback {

        private static final int LOOKBACK = 5;
        private static final long PRICE_NULL = Mbp10Decoder.priceNullValue();

        private final long[] midPrices = new long[LOOKBACK];
        private int tickCount = 0;
        int signalCount = 0;

        @Override
        public List<Intent> onMarketData(Schema data) {
            if (!(data instanceof Mbp10Schema mbp10)) {
                return List.of();
            }

            long bid = mbp10.decoder.bidPrice0();
            long ask = mbp10.decoder.askPrice0();
            if (bid == PRICE_NULL || ask == PRICE_NULL || bid <= 0 || ask <= 0) {
                return List.of();
            }

            long mid = (bid + ask) / 2;
            int slot = tickCount % LOOKBACK;
            long prevMid = midPrices[slot];
            midPrices[slot] = mid;
            tickCount++;

            if (tickCount <= LOOKBACK || prevMid <= 0) {
                return List.of();
            }

            if (mid == prevMid) {
                return List.of();
            }

            signalCount++;
            Side takeSide = mid > prevMid ? Side.Bid : Side.Ask;
            return List.of(buildTakeIntent(mbp10, takeSide));
        }

        @Override
        public List<Intent> onExecutionReport(OrderExecutionReport report) {
            return List.of();
        }

        @Override
        public long simulateProcessingTime() {
            return 500_000L;
        }

        private static Intent buildTakeIntent(Mbp10Schema schema, Side takeSide) {
            Intent intent = new Intent();
            intent.encoder
                    .exchangeId((short) schema.decoder.exchangeId())
                    .securityId(schema.decoder.securityId())
                    .strategyId(0)
                    .bidPrice(IntentDecoder.bidPriceNullValue())
                    .bidSize(IntentDecoder.bidSizeNullValue())
                    .askPrice(IntentDecoder.askPriceNullValue())
                    .askSize(IntentDecoder.askSizeNullValue())
                    .takeSide(takeSide)
                    .takeSize(1L)
                    .takeOrderType(OrderType.MARKET)
                    .takeLimitPrice(IntentDecoder.takeLimitPriceNullValue());
            intent.encoder.flags().clear();
            return intent;
        }
    }
}
