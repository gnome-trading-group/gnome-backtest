package group.gnometrading.backtest.book;

public record LocalOrderFill(LocalOrder localOrder, long fillSize, long remainingAfterFill) {}
