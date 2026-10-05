package group.gnometrading.backtest.config;

import group.gnometrading.backtest.driver.SimulatedClock;
import group.gnometrading.oms.pnl.PriceSlotRegistry;
import group.gnometrading.oms.pnl.SharedPriceBuffer;

/**
 * What a backtest's OMS and driver must share, so they see one instance as they do in live trading: the price buffer
 * the OMS, its risk policies and the {@code PriceWriterAgent} all use, and the simulated clock the OMS stamps its
 * reports with and the driver advances.
 */
public record BacktestContext(SharedPriceBuffer priceBuffer, PriceSlotRegistry priceRegistry, SimulatedClock clock) {}
