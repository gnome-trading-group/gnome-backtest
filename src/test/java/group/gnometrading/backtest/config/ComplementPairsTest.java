package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.backtest.config.BacktestDriverFactory.ResolvedListing;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComplementPairsTest {

    private static ResolvedListing listing(int listingId, String exchangeCode, String exchangeSecurityId) {
        return new ResolvedListing(
                new Listing(
                        listingId,
                        new Exchange(1, exchangeCode, exchangeCode, "eu-west-1"),
                        null,
                        exchangeSecurityId,
                        "x"),
                null);
    }

    private static BacktestConfig config(boolean complementFills) {
        BacktestConfig config = new BacktestConfig();
        config.complementFills = complementFills;
        return config;
    }

    @Test
    void bothOutcomesOfAPolymarketInternationalMarketPairBothWays() {
        Map<Integer, Integer> pairs = BacktestDriverFactory.complementPairs(
                config(true),
                List.of(
                        listing(1, "POLYMARKET_INTL", "0xaaa:11"),
                        listing(2, "POLYMARKET_INTL", "0xaaa:22"),
                        listing(3, "KALSHI", "KXCS2GAME-26NOV17-FAL")));
        assertEquals(Map.of(1, 2, 2, 1), pairs);
    }

    @Test
    void anOutcomeWhoseOtherOutcomeTheRunDoesNotTradeStaysUnpaired() {
        assertTrue(BacktestDriverFactory.complementPairs(
                        config(true),
                        List.of(listing(1, "POLYMARKET_INTL", "0xaaa:11"), listing(2, "POLYMARKET_INTL", "0xbbb:22")))
                .isEmpty());
    }

    @Test
    void polymarketUsListingsAreNeverPaired() {
        assertTrue(BacktestDriverFactory.complementPairs(
                        config(true),
                        List.of(
                                listing(1, "POLYMARKET_US", "aec-cs2-fal-jum-2026-11-17:long"),
                                listing(2, "POLYMARKET_US", "aec-cs2-fal-jum-2026-11-17:short")))
                .isEmpty());
    }

    @Test
    void switchedOffNothingPairs() {
        assertTrue(BacktestDriverFactory.complementPairs(
                        config(false),
                        List.of(listing(1, "POLYMARKET_INTL", "0xaaa:11"), listing(2, "POLYMARKET_INTL", "0xaaa:22")))
                .isEmpty());
    }

    @Test
    void theSwitchIsOnByDefault() {
        assertTrue(new BacktestConfig().complementFills);
    }
}
