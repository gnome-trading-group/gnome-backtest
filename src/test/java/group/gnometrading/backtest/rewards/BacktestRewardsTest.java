package group.gnometrading.backtest.rewards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import group.gnometrading.schemas.book.Action;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.sequencer.metrics.ColumnDef;
import group.gnometrading.sequencer.metrics.RecordBuffer;
import group.gnometrading.simulation.book.SelfTradePrevention;
import group.gnometrading.simulation.exchange.MbpSimulatedExchange;
import group.gnometrading.simulation.latency.StaticLatency;
import group.gnometrading.simulation.queues.OptimisticQueueModel;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import group.gnometrading.sm.incentive.IncentiveProgram;
import group.gnometrading.sm.incentive.IncentiveTimeline;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A backtest's rewards from what a real simulated exchange did with our orders: they are scored where and when they
 * rested on its book, against the books it saw.
 */
class BacktestRewardsTest {

    private static final long SECOND = 1_000_000_000L;
    private static final long HOUR = 3600 * SECOND;
    private static final long CENT = 10_000_000L;
    private static final long DOLLAR = 100 * CENT;
    private static final long CONTRACT = 1_000_000L;
    private static final long PERIOD = 10_000 * SECOND;
    private static final long T0 = 1_790_002_800L * SECOND;
    private static final int EXCHANGE_ID = 3;
    private static final int SECURITY_ID = 42;
    private static final int LISTING_ID = 700;

    private final MbpSimulatedExchange exchange = new MbpSimulatedExchange(
            (now, buy, price, quantity, maker, state) -> 0,
            new StaticLatency(0),
            new StaticLatency(0),
            new OptimisticQueueModel(),
            SelfTradePrevention.CANCEL_INCOMING);
    private final BacktestRecorder recorder = new BacktestRecorder();
    private final BacktestRewards rewards = new BacktestRewards(recorder, HOUR);

    private static Listing kalshi(final String exchangeSecurityId) {
        return new Listing(
                LISTING_ID,
                new Exchange(EXCHANGE_ID, "KALSHI", "Kalshi", "us-east-1"),
                new Security(
                        SECURITY_ID,
                        exchangeSecurityId,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        false,
                        false,
                        0,
                        0,
                        true,
                        0),
                exchangeSecurityId,
                exchangeSecurityId);
    }

    /** $100 over 10,000 s from T0, 1,000 contracts a side, no discount. */
    private static IncentiveTimeline program() {
        return new IncentiveTimeline(List.<IncentiveProgram[]>of(new IncentiveProgram[] {
            new IncentiveProgram(
                    "k1",
                    IncentiveProgram.IncentiveKind.LIQUIDITY,
                    "KXRT-VIO-60",
                    T0,
                    T0 + PERIOD,
                    100 * DOLLAR,
                    IncentiveProgram.RewardPer.PERIOD,
                    JsonNodeFactory.instance
                            .objectNode()
                            .put("targetSize", 1000)
                            .put("discountFactor", 1.0),
                    false,
                    T0)
        }));
    }

    private void bindYes() {
        assertTrue(rewards.bind(kalshi("KXRT-VIO-60"), program(), nanos -> CENT));
        rewards.begin(T0);
    }

    /** A book with one level a side; a side of null price is empty. */
    private void book(final long at, final long bidCents, final long askCents, final boolean bad) {
        final Mbp10Schema schema = new Mbp10Schema();
        final long nullPrice = Mbp10Decoder.priceNullValue();
        final long nullSize = Mbp10Decoder.sizeNullValue();
        schema.encoder.timestampEvent(at).action(Action.Add).price(nullPrice).size(nullSize);
        schema.encoder
                .bidPrice0(bidCents < 0 ? nullPrice : bidCents * CENT)
                .bidSize0(bidCents < 0 ? nullSize : 2000 * CONTRACT)
                .askPrice0(askCents < 0 ? nullPrice : askCents * CENT)
                .askSize0(askCents < 0 ? nullSize : 2000 * CONTRACT);
        schema.encoder
                .bidPrice1(nullPrice)
                .bidSize1(nullSize)
                .askPrice1(nullPrice)
                .askSize1(nullSize);
        schema.encoder
                .bidPrice2(nullPrice)
                .bidSize2(nullSize)
                .askPrice2(nullPrice)
                .askSize2(nullSize);
        schema.encoder
                .bidPrice3(nullPrice)
                .bidSize3(nullSize)
                .askPrice3(nullPrice)
                .askSize3(nullSize);
        schema.encoder
                .bidPrice4(nullPrice)
                .bidSize4(nullSize)
                .askPrice4(nullPrice)
                .askSize4(nullSize);
        schema.encoder
                .bidPrice5(nullPrice)
                .bidSize5(nullSize)
                .askPrice5(nullPrice)
                .askSize5(nullSize);
        schema.encoder
                .bidPrice6(nullPrice)
                .bidSize6(nullSize)
                .askPrice6(nullPrice)
                .askSize6(nullSize);
        schema.encoder
                .bidPrice7(nullPrice)
                .bidSize7(nullSize)
                .askPrice7(nullPrice)
                .askSize7(nullSize);
        schema.encoder
                .bidPrice8(nullPrice)
                .bidSize8(nullSize)
                .askPrice8(nullPrice)
                .askSize8(nullSize);
        schema.encoder
                .bidPrice9(nullPrice)
                .bidSize9(nullSize)
                .askPrice9(nullPrice)
                .askSize9(nullSize);
        schema.encoder.flags().clear().badData(bad);
        rewards.onMarketData(at, LISTING_ID, schema);
        report(at, exchange.onMarketData(schema));
    }

    private void report(final long at, final List<OrderExecutionReport> reports) {
        for (final OrderExecutionReport r : reports) {
            rewards.onReport(at, exchange, r);
        }
    }

    private void submit(final long at, final long clientOid, final Side side, final long cents, final long contracts) {
        final Order order = new Order();
        order.encoder
                .exchangeId((short) EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .price(cents * CENT)
                .size(contracts * CONTRACT)
                .side(side)
                .orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GOOD_TILL_CANCELED);
        order.encodeClientOid(clientOid, 0);
        final List<OrderExecutionReport> reports = new ArrayList<>(exchange.submitOrder(order, at));
        reports.addAll(exchange.processDue(at));
        report(at, reports);
    }

    private void cancel(final long at, final long clientOid) {
        final CancelOrder cancel = new CancelOrder();
        cancel.encoder.exchangeId((short) EXCHANGE_ID).securityId(SECURITY_ID);
        cancel.encodeClientOid(clientOid, 0);
        final List<OrderExecutionReport> reports = new ArrayList<>(exchange.cancelOrder(cancel, at));
        reports.addAll(exchange.processDue(at));
        report(at, reports);
    }

    /** The long or int column {@code name} of the recorder's last reward row. */
    private long last(final String name) {
        final RecordBuffer buffer = recorder.getRewardRecords();
        final int row = buffer.getCount() - 1;
        for (final ColumnDef column : buffer.getColumns()) {
            if (column.name().equals(name)) {
                return column.type() == ColumnDef.ColumnType.INT
                        ? buffer.getIntColumn(column.columnIndex())[row]
                        : buffer.getLongColumn(column.columnIndex())[row];
            }
        }
        throw new IllegalArgumentException(name);
    }

    @Test
    void ordersRestingOnTheExchangeEarnKalshisWorkedExample() {
        bindYes();
        book(T0, 50, 52, false);
        // 500 a side: a tick better than everyone's 2,000 bid, and joining their 2,000 ask (a 51c ask would cross our
        // own bid). With no discount and the whole level counting, 20% of each side.
        submit(T0, 1, Side.Bid, 51, 500);
        submit(T0, 2, Side.Ask, 52, 500);
        book(T0 + 8000 * SECOND, 50, -1, false);
        rewards.finish(T0 + PERIOD);

        assertEquals(16 * DOLLAR, last("gross"), 1);
        assertEquals(16 * DOLLAR, last("market_paid"));
        assertEquals(8000 * SECOND, last("counting_nanos"));
        assertEquals(LISTING_ID, last("listing_id"));
    }

    @Test
    void anOrderStopsEarningWhenTheExchangeCancelsIt() {
        bindYes();
        book(T0, 50, 52, false);
        submit(T0, 1, Side.Bid, 51, 500);
        cancel(T0 + 5000 * SECOND, 1);
        rewards.finish(T0 + PERIOD);

        // 20% of the YES side for half the period: $100 × 0.2 × 0.5 ÷ 2.
        assertEquals(5 * DOLLAR, last("gross"), 1);
    }

    @Test
    void aBookFlaggedBadScoresNothingUntilAGoodOne() {
        bindYes();
        book(T0, 50, 52, false);
        submit(T0, 1, Side.Bid, 51, 500);
        book(T0 + 2000 * SECOND, 50, 52, true);
        book(T0 + 6000 * SECOND, 50, 52, false);
        rewards.finish(T0 + PERIOD);

        assertEquals(4000 * SECOND, last("no_data_nanos"));
        assertEquals(6 * DOLLAR, last("gross"), 1);
    }

    @Test
    void snapshotsAreTakenEachIntervalAndAtTheEnd() {
        bindYes();
        book(T0, 50, 52, false);
        submit(T0, 1, Side.Bid, 51, 500);
        rewards.finish(T0 + PERIOD);

        // T0 is on the hour: snapshots at +1 h, +2 h, +3 h and the end, 2.78 h in.
        assertEquals(3, recorder.getRewardRecords().getCount());
        assertEquals(T0 + PERIOD, last("timestamp"));
    }

    @Test
    void programsOutsideTheRunAreLeftOutAndTimeBeforeItIsNotScored() {
        assertTrue(rewards.bind(kalshi("KXRT-VIO-60"), program(), nanos -> CENT));
        // A run starting after the program's period: nothing of it is recorded.
        rewards.begin(T0 + PERIOD + HOUR);
        rewards.finish(T0 + PERIOD + 2 * HOUR);
        assertEquals(0, recorder.getRewardRecords().getCount());
    }

    @Test
    void aRunStartingMidPeriodScoresFromItsStart() {
        assertTrue(rewards.bind(kalshi("KXRT-VIO-60"), program(), nanos -> CENT));
        rewards.begin(T0 + 5000 * SECOND);
        book(T0 + 6000 * SECOND, 50, 52, false);
        rewards.finish(T0 + PERIOD);

        assertEquals(1000 * SECOND, last("no_data_nanos"));
        assertEquals(4000 * SECOND, last("counting_nanos"));
    }

    @Test
    void aVenueWithoutAProgramTheSimulationScoresIsNotBound() {
        final Listing polymarket = new Listing(
                LISTING_ID,
                new Exchange(EXCHANGE_ID, "POLYMARKET_INTL", "Polymarket", "eu-west-1"),
                null,
                "0xabc:1",
                "");
        assertFalse(rewards.bind(polymarket, program(), nanos -> CENT));
        assertFalse(rewards.bind(kalshi("KXRT-VIO-60"), new IncentiveTimeline(List.of()), nanos -> CENT));
        assertTrue(rewards.isEmpty());
    }
}
