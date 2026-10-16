package group.gnometrading.backtest.config;

import group.gnometrading.simulation.config.SimulationOverrides;

public class ListingSimConfig {

    public int listingId;
    /** Blocks this listing simulates with in place of its exchange's; null for none. */
    public SimulationOverrides overrides;
}
