package group.gnometrading.backtest;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.backtest.exchange.MbpSimulatedExchange;
import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class GoldenFixtureTest {

    private static final long PRICE_NULL = Long.MIN_VALUE;
    private static final long SIZE_NULL = 4294967295L;

    // ---- Jackson POJOs ----

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FixtureLevel(long bid_px, long bid_sz, long ask_px, long ask_sz) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FixtureOrder(String side, long price, long size, String client_oid, String order_type, String tif) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FixtureReport(String exec_type, String order_status, String client_oid, Long filled_qty, Long leaves_qty) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FixtureEvent(
            String type,
            FixtureOrder order,
            String side,
            Long price,
            Long size,
            String client_oid,
            List<FixtureReport> expected_reports) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Fixture(String name, String description, List<FixtureLevel> initial_levels, List<FixtureEvent> events) {}

    // ---- Dummy models ----

    static class ZeroFeeModel implements FeeModel {
        @Override
        public double calculateFee(double totalPrice, boolean isMaker) {
            return 0.0;
        }
    }

    static class ZeroLatency implements LatencyModel {
        @Override
        public long simulate() {
            return 0;
        }
    }

    static class PassthroughQueueModel implements QueueModel {
        @Override
        public void onModify(long prev, long next, ArrayDeque<group.gnometrading.backtest.book.LocalOrder> q) {}
    }

    // ---- Test discovery ----

    @TestFactory
    Stream<DynamicTest> goldenFixtures() throws IOException, URISyntaxException {
        URL fixturesUrl = getClass().getClassLoader().getResource("fixtures");
        Objects.requireNonNull(fixturesUrl, "fixtures/ resource directory not found");
        Path fixturesDir = Paths.get(fixturesUrl.toURI());

        return Files.list(fixturesDir)
                .filter(p -> p.toString().endsWith(".json"))
                .sorted()
                .map(path -> DynamicTest.dynamicTest(
                        path.getFileName().toString().replace(".json", ""), () -> runFixture(path)));
    }

    private void runFixture(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Fixture fixture = mapper.readValue(path.toFile(), Fixture.class);

        MbpSimulatedExchange exchange = new MbpSimulatedExchange(
                new ZeroFeeModel(), new ZeroLatency(), new ZeroLatency(), new PassthroughQueueModel());

        // Seed initial book state
        if (fixture.initial_levels() != null && !fixture.initial_levels().isEmpty()) {
            Mbp10Schema init = buildAddSchema(fixture.initial_levels());
            exchange.onMarketData(init);
        }

        for (int i = 0; i < fixture.events().size(); i++) {
            FixtureEvent event = fixture.events().get(i);
            List<BacktestExecutionReport> actual;

            switch (event.type()) {
                case "submit_order" -> {
                    BacktestOrder order = toOrder(event.order());
                    actual = exchange.submitOrder(order);
                }
                case "cancel_order" -> {
                    BacktestCancelOrder cancel = new BacktestCancelOrder(1, 1, event.client_oid());
                    actual = exchange.cancelOrder(cancel);
                }
                case "trade" -> {
                    Mbp10Schema trade = buildTradeSchema(event.price(), event.size(), toSide(event.side()));
                    actual = exchange.onMarketData(trade);
                }
                case "market_update" -> {
                    Mbp10Schema update = buildAddSchema(event.order() != null ? List.of() : fixture.initial_levels());
                    actual = exchange.onMarketData(update);
                }
                default -> throw new IllegalArgumentException("Unknown event type: " + event.type());
            }

            assertReports(fixture.name(), i, event.expected_reports(), actual);
        }
    }

    private void assertReports(
            String fixtureName, int eventIdx, List<FixtureReport> expected, List<BacktestExecutionReport> actual) {
        assertEquals(
                expected.size(),
                actual.size(),
                fixtureName + " event[" + eventIdx + "]: expected " + expected.size() + " reports but got "
                        + actual.size());

        for (int j = 0; j < expected.size(); j++) {
            FixtureReport exp = expected.get(j);
            BacktestExecutionReport act = actual.get(j);
            String ctx = fixtureName + " event[" + eventIdx + "] report[" + j + "]";

            assertEquals(toExecType(exp.exec_type()), act.execType, ctx + " execType");
            assertEquals(toOrderStatus(exp.order_status()), act.orderStatus, ctx + " orderStatus");

            if (exp.client_oid() != null) {
                assertEquals(exp.client_oid(), act.clientOid, ctx + " clientOid");
            }
            if (exp.filled_qty() != null) {
                assertEquals(exp.filled_qty(), act.filledQty, ctx + " filledQty");
            }
            if (exp.leaves_qty() != null) {
                assertEquals(exp.leaves_qty(), act.leavesQty, ctx + " leavesQty");
            }
        }
    }

    // ---- Schema builders ----

    private static Mbp10Schema buildAddSchema(List<FixtureLevel> levels) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Add);
        schema.encoder.side(Side.None);
        schema.encoder.price(PRICE_NULL);
        schema.encoder.size(SIZE_NULL);
        for (int i = 0; i < 10; i++) {
            FixtureLevel level = i < levels.size() ? levels.get(i) : null;
            setLevel(schema, i, level);
        }
        return schema;
    }

    private static Mbp10Schema buildTradeSchema(long price, long size, Side side) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Trade);
        schema.encoder.side(side);
        schema.encoder.price(price);
        schema.encoder.size(size);
        for (int i = 0; i < 10; i++) {
            setLevel(schema, i, null);
        }
        return schema;
    }

    private static void setLevel(Mbp10Schema schema, int idx, FixtureLevel level) {
        long bp = level != null ? level.bid_px() : PRICE_NULL;
        long bs = level != null ? level.bid_sz() : SIZE_NULL;
        long ap = level != null ? level.ask_px() : PRICE_NULL;
        long as_ = level != null ? level.ask_sz() : SIZE_NULL;
        switch (idx) {
            case 0 -> schema.encoder.bidPrice0(bp).bidSize0(bs).askPrice0(ap).askSize0(as_);
            case 1 -> schema.encoder.bidPrice1(bp).bidSize1(bs).askPrice1(ap).askSize1(as_);
            case 2 -> schema.encoder.bidPrice2(bp).bidSize2(bs).askPrice2(ap).askSize2(as_);
            case 3 -> schema.encoder.bidPrice3(bp).bidSize3(bs).askPrice3(ap).askSize3(as_);
            case 4 -> schema.encoder.bidPrice4(bp).bidSize4(bs).askPrice4(ap).askSize4(as_);
            case 5 -> schema.encoder.bidPrice5(bp).bidSize5(bs).askPrice5(ap).askSize5(as_);
            case 6 -> schema.encoder.bidPrice6(bp).bidSize6(bs).askPrice6(ap).askSize6(as_);
            case 7 -> schema.encoder.bidPrice7(bp).bidSize7(bs).askPrice7(ap).askSize7(as_);
            case 8 -> schema.encoder.bidPrice8(bp).bidSize8(bs).askPrice8(ap).askSize8(as_);
            case 9 -> schema.encoder.bidPrice9(bp).bidSize9(bs).askPrice9(ap).askSize9(as_);
        }
    }

    // ---- Type converters ----

    private static BacktestOrder toOrder(FixtureOrder o) {
        return new BacktestOrder(
                1,
                1,
                o.client_oid(),
                toSide(o.side()),
                o.price(),
                o.size(),
                toOrderType(o.order_type()),
                toTif(o.tif()));
    }

    private static Side toSide(String s) {
        return switch (s) {
            case "Bid" -> Side.Bid;
            case "Ask" -> Side.Ask;
            default -> Side.None;
        };
    }

    private static OrderType toOrderType(String s) {
        return switch (s) {
            case "MARKET" -> OrderType.MARKET;
            default -> OrderType.LIMIT;
        };
    }

    private static TimeInForce toTif(String s) {
        return switch (s) {
            case "IOC" -> TimeInForce.IMMEDIATE_OR_CANCELED;
            case "FOK" -> TimeInForce.FILL_OR_KILL;
            default -> TimeInForce.GOOD_TILL_CANCELED;
        };
    }

    private static ExecType toExecType(String s) {
        return switch (s) {
            case "NEW" -> ExecType.NEW;
            case "FILL" -> ExecType.FILL;
            case "PARTIAL_FILL" -> ExecType.PARTIAL_FILL;
            case "CANCEL" -> ExecType.CANCEL;
            case "REJECT" -> ExecType.REJECT;
            default -> throw new IllegalArgumentException("Unknown exec_type: " + s);
        };
    }

    private static OrderStatus toOrderStatus(String s) {
        return switch (s) {
            case "NEW" -> OrderStatus.NEW;
            case "PARTIALLY_FILLED" -> OrderStatus.PARTIALLY_FILLED;
            case "FILLED" -> OrderStatus.FILLED;
            case "CANCELED" -> OrderStatus.CANCELED;
            case "REJECTED" -> OrderStatus.REJECTED;
            default -> throw new IllegalArgumentException("Unknown order_status: " + s);
        };
    }
}
