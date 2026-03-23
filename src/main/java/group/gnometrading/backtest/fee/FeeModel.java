package group.gnometrading.backtest.fee;

public interface FeeModel {
    double calculateFee(double notional, boolean isMaker);
}
