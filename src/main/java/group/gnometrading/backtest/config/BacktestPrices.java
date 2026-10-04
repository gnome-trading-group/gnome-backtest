package group.gnometrading.backtest.config;

import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.SharedPriceBuffer;

/**
 * The price buffer a backtest shares between the OMS, its risk policies and the {@code PriceWriterAgent} that
 * feeds it, so all three see one instance as they do in live trading.
 */
public record BacktestPrices(SharedPriceBuffer buffer, PriceSlotRegistry registry) {}
