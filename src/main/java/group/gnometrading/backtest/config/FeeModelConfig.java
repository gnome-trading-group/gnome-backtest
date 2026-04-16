package group.gnometrading.backtest.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.fee.StaticFeeModel;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = FeeModelConfig.Static.class)
@JsonSubTypes({@JsonSubTypes.Type(value = FeeModelConfig.Static.class, name = "static")})
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
}
