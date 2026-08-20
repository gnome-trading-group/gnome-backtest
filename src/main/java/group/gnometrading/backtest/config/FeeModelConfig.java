package group.gnometrading.backtest.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import group.gnometrading.simulation.fee.FeeModel;
import group.gnometrading.simulation.fee.ParametricFeeModel;
import group.gnometrading.simulation.fee.StaticFeeModel;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = FeeModelConfig.Static.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = FeeModelConfig.Static.class, name = "static"),
    @JsonSubTypes.Type(value = FeeModelConfig.Parametric.class, name = "parametric")
})
public abstract class FeeModelConfig {

    public abstract FeeModel toModel();

    public static final class Static extends FeeModelConfig {
        public double takerFee;
        public double makerFee;

        @Override
        public FeeModel toModel() {
            return new StaticFeeModel(takerFee, makerFee);
        }
    }

    public static final class Parametric extends FeeModelConfig {
        public double takerFeeRate = 0.07;
        public double makerFeeRate = 0.0;

        @Override
        public FeeModel toModel() {
            return new ParametricFeeModel(takerFeeRate, makerFeeRate);
        }
    }
}
