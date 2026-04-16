package group.gnometrading.backtest.config;

import java.util.LinkedHashMap;
import java.util.Map;

public class RiskConfig {

    /**
     * Risk policies keyed by {@code RiskPolicyType} enum name (e.g. {@code "MAX_NOTIONAL"}).
     * Values are parameter maps whose keys match the policy's JSON parameter names.
     *
     * <p>Example:
     * <pre>
     * MAX_NOTIONAL:
     *   maxNotionalValue: 100000
     * KILL_SWITCH: {}
     * </pre>
     */
    public Map<String, Map<String, Object>> policies = new LinkedHashMap<>();
}
