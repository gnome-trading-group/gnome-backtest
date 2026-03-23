package group.gnometrading.backtest.fee;

public class StaticFeeModel implements FeeModel {

    private final double takerFee;
    private final double makerFee;

    public StaticFeeModel(double takerFee, double makerFee) {
        this.takerFee = takerFee;
        this.makerFee = makerFee;
    }

    @Override
    public double calculateFee(double notional, boolean isMaker) {
        return isMaker ? notional * makerFee : notional * takerFee;
    }
}
