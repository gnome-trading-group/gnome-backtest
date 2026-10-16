package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.simulation.config.LatencyConfig;
import group.gnometrading.simulation.config.QueueModelConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BacktestConfigTest {

    @Test
    void riskPoliciesAndStrategyIdReadFromYaml(@TempDir Path dir) throws Exception {
        Path yaml = dir.resolve("config.yaml");
        Files.writeString(
                yaml,
                """
                strategy_id: 12
                risk:
                  from_registry: true
                  policies:
                    - type: MAX_ORDER_NOTIONAL
                      params:
                        maxOrderNotional: 1000
                    - type: MAX_OPEN_ORDERS
                      strategy_id: 12
                      listing_id: 7122
                      params:
                        maxOpenOrders: 3
                """);

        BacktestConfig config = BacktestConfig.fromYaml(yaml);

        assertEquals(12, config.strategyId);
        assertTrue(config.risk.fromRegistry);
        assertEquals(2, config.risk.policies.size());
        RiskConfig.PolicyConfig global = config.risk.policies.get(0);
        assertEquals("MAX_ORDER_NOTIONAL", global.type);
        assertEquals(0, global.strategyId);
        assertEquals(0, global.listingId);
        RiskConfig.PolicyConfig scoped = config.risk.policies.get(1);
        assertEquals(12, scoped.strategyId);
        assertEquals(7122, scoped.listingId);
        assertEquals(3, ((Number) scoped.params.get("maxOpenOrders")).intValue());
    }

    @Test
    void simulationOverridesReadPerExchangeAndPerListing(@TempDir Path dir) throws Exception {
        Path yaml = dir.resolve("config.yaml");
        Files.writeString(
                yaml,
                """
                simulation_as_of: "2026-10-15T00:00:00Z"
                exchanges:
                  KALSHI:
                    queue_model: {type: optimistic}
                listings:
                  - listing_id: 48211
                  - listing_id: 48212
                    overrides:
                      network_latency: {type: static, latency_nanos: 1}
                """);

        BacktestConfig config = BacktestConfig.fromYaml(yaml);

        assertEquals("2026-10-15T00:00:00Z", config.simulationAsOf);
        assertInstanceOf(QueueModelConfig.Optimistic.class, config.exchanges.get("KALSHI").queueModel);
        assertNull(config.exchanges.get("KALSHI").networkLatency);
        assertNull(config.listings.get(0).overrides);
        LatencyConfig.Static network =
                assertInstanceOf(LatencyConfig.Static.class, config.listings.get(1).overrides.networkLatency);
        assertEquals(1L, network.latencyNanos);
    }

    // A block without a type used to become a static latency of 0 or a risk-averse queue, whatever was meant.
    @Test
    void aBlockMustNameItsModel(@TempDir Path dir) throws Exception {
        Path yaml = dir.resolve("config.yaml");
        Files.writeString(
                yaml,
                """
                exchanges:
                  KALSHI:
                    network_latency: {latency_nanos: 5}
                """);

        assertThrows(Exception.class, () -> BacktestConfig.fromYaml(yaml));
    }
}
