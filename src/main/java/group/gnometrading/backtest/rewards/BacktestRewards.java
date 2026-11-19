package group.gnometrading.backtest.rewards;

import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.simulation.book.LocalOrder;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.simulation.rewards.PriceGrids;
import group.gnometrading.simulation.rewards.RewardAccrual;
import group.gnometrading.simulation.rewards.RewardScorer;
import group.gnometrading.simulation.rewards.venues.RewardVenues;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.incentive.IncentiveProgram;
import group.gnometrading.sm.incentive.IncentiveTimeline;
import java.util.EnumSet;
import java.util.Set;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2LongHashMap;

/**
 * Estimates the venue liquidity rewards a backtest's resting orders earn. The driver hands it the exchange-side books
 * and every report the simulated exchange makes, both at venue time, so our orders are scored where and when they
 * rested. Snapshots of what has accrued go to the recorder every interval of simulated time and at the end.
 */
public final class BacktestRewards {

    private static final int ORDERS_PER_MARKET = 64;
    private static final int ORDERS = 4096;
    private static final long NONE = -1;
    private static final Set<RecordType> SHALLOW_BOOKS =
            EnumSet.of(RecordType.MBP_1, RecordType.BBO_1S, RecordType.BBO_1M);

    private final RewardScorer scorer = new RewardScorer(RewardScorer.Mode.INSERT_OURS, ORDERS_PER_MARKET, ORDERS);
    private final Long2LongHashMap listingsByInstrument = new Long2LongHashMap(NONE);
    private final IntHashSet bound = new IntHashSet();
    private final BacktestRecorder recorder;
    private final long snapshotIntervalNanos;
    private long nextSnapshot = Long.MIN_VALUE;
    private long runStart = Long.MIN_VALUE;

    public BacktestRewards(final BacktestRecorder recorder, final long snapshotIntervalNanos) {
        if (snapshotIntervalNanos <= 0) {
            throw new IllegalArgumentException("The rewards snapshot interval must be positive");
        }
        this.recorder = recorder;
        this.snapshotIntervalNanos = snapshotIntervalNanos;
    }

    /**
     * Scores the listing when its venue's program is supported and it has programs in the run's registry view.
     * Returns whether it was bound.
     */
    public boolean bind(final Listing listing, final IncentiveTimeline programs, final PriceGrids grids) {
        final RewardVenues.Binding binding = RewardVenues.of(listing);
        if (binding == null || programs.size() == 0) {
            return false;
        }
        this.scorer.bind(
                listing.listingId(), binding.marketKey(), binding.complement(), binding.rule(), programs, grids);
        this.listingsByInstrument.put(
                instrument(listing.exchange().exchangeId(), listing.security().securityId()), listing.listingId());
        this.bound.add(listing.listingId());
        return true;
    }

    public boolean isEmpty() {
        return this.bound.isEmpty();
    }

    /**
     * Starts scoring at the run's start, once every listing is bound: earlier time is outside the run, and programs
     * that ended before it are left out of the snapshots.
     */
    public void begin(final long runStartNanos) {
        this.runStart = runStartNanos;
        this.scorer.beginAt(runStartNanos);
    }

    /**
     * A record of a listing's market data, as the exchange sees it. A book flagged bad (data went missing) and a whole
     * book the scorer can't read (shallower than MBP-10) score nothing until the next good MBP-10 book; trades and bars
     * change nothing.
     */
    public void onMarketData(final long timestamp, final int listingId, final StreamRecord record) {
        snapshotsBefore(timestamp);
        if (!this.bound.contains(listingId)) {
            return;
        }
        if (record.isBadData()) {
            this.scorer.onGap(listingId, timestamp);
        } else if (record instanceof Mbp10Schema book) {
            this.scorer.onBook(listingId, book.decoder);
        } else if (SHALLOW_BOOKS.contains(record.recordType)) {
            this.scorer.onGap(listingId, timestamp);
        }
    }

    /** A report the exchange made at {@code timestamp}: the order now rests as the exchange holds it, or is gone. */
    public void onReport(final long timestamp, final SimulatedExchange exchange, final OrderExecutionReport report) {
        snapshotsBefore(timestamp);
        final long listingId =
                this.listingsByInstrument.get(instrument(report.decoder.exchangeId(), report.decoder.securityId()));
        if (listingId == NONE) {
            return;
        }
        final long clientOid = report.getClientOidCounter();
        final LocalOrder resting = exchange.restingOrder(clientOid);
        if (resting == null) {
            this.scorer.onOrder((int) listingId, clientOid, false, 0, 0, timestamp);
        } else {
            this.scorer.onOrder(
                    (int) listingId,
                    clientOid,
                    resting.order.decoder.side() == Side.Bid,
                    resting.order.decoder.price(),
                    resting.remaining,
                    timestamp);
        }
    }

    /** Accrues up to the run's end and records the final snapshot there. */
    public void finish(final long endNanos) {
        snapshotsBefore(endNanos);
        this.scorer.advanceTo(endNanos);
        record(endNanos);
    }

    /** Takes every snapshot due at or before {@code timestamp}, before the event there changes anything. */
    private void snapshotsBefore(final long timestamp) {
        if (this.nextSnapshot == Long.MIN_VALUE) {
            this.nextSnapshot = (Math.floorDiv(timestamp, this.snapshotIntervalNanos) + 1) * this.snapshotIntervalNanos;
        }
        while (timestamp >= this.nextSnapshot) {
            this.scorer.advanceTo(this.nextSnapshot);
            record(this.nextSnapshot);
            this.nextSnapshot += this.snapshotIntervalNanos;
        }
    }

    /** Snapshots the programs whose period overlaps the run so far. */
    private void record(final long timestamp) {
        if (this.recorder == null) {
            return;
        }
        for (int i = 0; i < this.scorer.accrualCount(); i++) {
            final RewardAccrual accrual = this.scorer.accrual(i);
            final IncentiveProgram program = accrual.program();
            if (program.periodStartNanos() < timestamp && program.periodEndNanos() > this.runStart) {
                this.recorder.onRewards(timestamp, accrual);
            }
        }
    }

    public RewardScorer scorer() {
        return this.scorer;
    }

    private static long instrument(final int exchangeId, final long securityId) {
        return ((long) exchangeId << 32) | (securityId & 0xFFFF_FFFFL);
    }
}
