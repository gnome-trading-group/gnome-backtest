package group.gnometrading.backtest.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RiskConfig {

    /**
     * Also apply the strategy's live policies from the registry, as production fetches them for {@code strategy_id}
     * outside a session. Kill switches are not loaded: they are the live strategy's state, not a limit to test.
     */
    public boolean fromRegistry = false;

    /**
     * Risk policies, targeted as the registry targets them: an omitted or 0 id means every strategy, exchange or
     * listing, so a policy with none applies to everything. A policy names an exchange or a listing, not both.
     *
     * <p>Example:
     * <pre>
     * - type: MAX_ORDER_NOTIONAL
     *   params:
     *     maxOrderNotional: 100000000000000   # $100,000 in price units (1e9 = $1)
     * - type: MAX_COMMITTED_CAPITAL
     *   exchange_id: 2
     *   params:
     *     maxCapital: 500000000000            # $500 tied up at once on exchange 2
     * - type: MAX_OPEN_ORDERS
     *   listing_id: 7122
     *   params:
     *     maxOpenOrders: 3
     * </pre>
     */
    public List<PolicyConfig> policies = new ArrayList<>();

    public static class PolicyConfig {
        /** A {@code RiskPolicyType} name, e.g. {@code "MAX_ORDER_NOTIONAL"}. */
        public String type;

        public int strategyId;
        public int exchangeId;
        public int listingId;
        /** Keys match the policy's JSON parameter names. */
        public Map<String, Object> params;
    }
}
