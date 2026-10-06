package group.gnometrading.backtest.driver;

import group.gnometrading.schemas.Bbo1mSchema;
import group.gnometrading.schemas.Bbo1sSchema;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.TradesSchema;
import group.gnometrading.simulation.config.LatencyConfig;
import group.gnometrading.simulation.latency.LatencyModel;

/** When one listing's market data reaches us, before its connection puts it in order. */
public final class MarketDataArrival {

    private final boolean recorded;
    private final LatencyModel model;

    private MarketDataArrival(boolean recorded, LatencyModel model) {
        this.recorded = recorded;
        this.model = model;
    }

    /**
     * A recorded config replays each record's receive time, falling back to its model for a record without one; any
     * other config adds a draw from its model to the event time.
     */
    public static MarketDataArrival of(LatencyConfig config, long seed) {
        return new MarketDataArrival(config instanceof LatencyConfig.Recorded, config.toModel(seed));
    }

    long arrival(Schema record, long eventTimestamp) {
        if (recorded) {
            long received = receivedAt(record);
            // A receive time before the event is a clock glitch, not a time the record could have arrived.
            if (received >= eventTimestamp) {
                return received;
            }
        }
        return eventTimestamp + model.simulate();
    }

    /** The record's receive time, or 0 when it has none. */
    private static long receivedAt(Schema record) {
        if (record instanceof Mbp10Schema mbp10) {
            return mbp10.decoder.timestampRecv();
        } else if (record instanceof Mbp1Schema mbp1) {
            return mbp1.decoder.timestampRecv();
        } else if (record instanceof MboSchema mbo) {
            return mbo.decoder.timestampRecv();
        } else if (record instanceof TradesSchema trades) {
            return trades.decoder.timestampRecv();
        } else if (record instanceof Bbo1sSchema bbo1s) {
            return bbo1s.decoder.timestampRecv();
        } else if (record instanceof Bbo1mSchema bbo1m) {
            return bbo1m.decoder.timestampRecv();
        }
        return 0;
    }
}
