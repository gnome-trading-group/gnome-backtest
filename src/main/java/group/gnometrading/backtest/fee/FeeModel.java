package group.gnometrading.backtest.fee;

public interface FeeModel {
    double calculateFee(long totalPrice, boolean isMaker);
}
