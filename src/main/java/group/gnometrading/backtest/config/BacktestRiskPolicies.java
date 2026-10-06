package group.gnometrading.backtest.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.RegistryConnection;
import group.gnometrading.oms.risk.Configurable;
import group.gnometrading.oms.risk.PolicyFactory;
import group.gnometrading.oms.risk.RiskEngine.ScopedPolicy;
import group.gnometrading.oms.risk.RiskPolicyType;
import group.gnometrading.risk.RiskMaster;
import group.gnometrading.risk.RiskPolicyRecord;
import group.gnometrading.strings.GnomeString;
import group.gnometrading.strings.ViewString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The risk policies a backtest runs under: the strategy's registry policies when asked for, then those in the
 * config. Unlike production, a policy that can't be built fails the run rather than killing its target, since a
 * backtest has no operator to see the kill.
 */
final class BacktestRiskPolicies {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BacktestRiskPolicies() {}

    static List<ScopedPolicy> load(
            final RiskConfig risk,
            final int strategyId,
            final RegistryConnection registry,
            final PolicyFactory factory) {
        final List<ScopedPolicy> policies = new ArrayList<>();
        if (risk == null) {
            return policies;
        }
        if (risk.fromRegistry) {
            loadFromRegistry(strategyId, registry, factory, policies);
        }
        for (RiskConfig.PolicyConfig policy : risk.policies) {
            policies.add(fromConfig(policy, factory));
        }
        return policies;
    }

    private static void loadFromRegistry(
            final int strategyId,
            final RegistryConnection registry,
            final PolicyFactory factory,
            final List<ScopedPolicy> policies) {
        if (strategyId == 0) {
            throw new IllegalArgumentException(
                    "risk.from_registry needs the strategy_id whose policies to load; 0 is every strategy");
        }
        if (registry == null) {
            throw new IllegalArgumentException("risk.from_registry needs a registry connection");
        }
        final RiskMaster master = new RiskMaster(registry, strategyId, null);
        master.refresh();
        for (int i = 0; i < master.getPolicyCount(); i++) {
            final RiskPolicyRecord record = master.getRecord(i);
            if (!record.enabled || record.sessionId.length() != 0) {
                continue;
            }
            final RiskPolicyType type = RiskPolicyType.fromString(record.policyType);
            if (type == null) {
                throw new IllegalArgumentException(
                        "Registry risk policy " + record.policyId + " has unknown type " + record.policyType);
            }
            if (type.category() != RiskPolicyType.Category.KILL) {
                policies.add(build(type, record.strategyId, record.listingId, record.parametersJson, factory));
            }
        }
    }

    private static ScopedPolicy fromConfig(final RiskConfig.PolicyConfig config, final PolicyFactory factory) {
        final RiskPolicyType type;
        try {
            type = RiskPolicyType.valueOf(config.type);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException("Unknown risk policy type " + config.type, e);
        }
        if (type.category() == RiskPolicyType.Category.KILL) {
            throw new IllegalArgumentException(
                    type + " stops all trading and has no meaning in a backtest; remove it from the risk config");
        }
        final String json;
        try {
            json = MAPPER.writeValueAsString(config.params != null ? config.params : Map.of());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize params for policy " + type, e);
        }
        return build(type, config.strategyId, config.listingId, new ViewString(json), factory);
    }

    private static ScopedPolicy build(
            final RiskPolicyType type,
            final int strategyId,
            final int listingId,
            final GnomeString params,
            final PolicyFactory factory) {
        // As in production: a policy with no listing judges the total across listings.
        final Configurable policy = factory.create(type, listingId == 0);
        try {
            policy.reconfigure(params);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Risk policy " + type + " can't read its parameters " + params, e);
        }
        return new ScopedPolicy(strategyId, listingId, policy);
    }
}
