package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                    - type: MAX_NOTIONAL
                      params:
                        maxNotionalValue: 1000
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
        assertEquals("MAX_NOTIONAL", global.type);
        assertEquals(0, global.strategyId);
        assertEquals(0, global.listingId);
        RiskConfig.PolicyConfig scoped = config.risk.policies.get(1);
        assertEquals(12, scoped.strategyId);
        assertEquals(7122, scoped.listingId);
        assertEquals(3, ((Number) scoped.params.get("maxOpenOrders")).intValue());
    }
}
