package group.gnometrading.backtest.exchange;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import group.gnometrading.backtest.fee.FeeModel;
import group.gnometrading.backtest.latency.LatencyModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.TimeInForce;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Parameterized scenario runner for MBP simulated exchanges only.
 *
 * <p>Loads all *.yaml fixtures from src/test/resources/fixtures/, drives MbpSimulatedExchange
 * through each event, and asserts the returned execution reports match expectations.
 */
class MbpExchangeScenarioTest {

    private static final long PRICE_NULL = Mbp10Decoder.priceNullValue();
    private static final long SIZE_NULL = Mbp10Decoder.sizeNullValue();

    // --- YAML POJOs ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Scenario {
        public String name;
        public String description;

        @JsonProperty("initial_levels")
        public List<Level> initialLevels;

        public List<Event> events;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Level {
        @JsonProperty("bid_px")
        public Long bidPx;

        @JsonProperty("bid_sz")
        public Long bidSz;

        @JsonProperty("ask_px")
        public Long askPx;

        @JsonProperty("ask_sz")
        public Long askSz;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Event {
        public String type;
        public OrderSpec order;
        public String side;
        public long price;
        public long size;

        @JsonProperty("client_oid")
        public String clientOid;

        @JsonProperty("new_price")
        public long newPrice;

        @JsonProperty("new_size")
        public long newSize;

        public List<Level> levels;

        @JsonProperty("expected_reports")
        public List<ExpectedReport> expectedReports;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class OrderSpec {
        public String side;
        public long price;
        public long size;

        @JsonProperty("client_oid")
        public String clientOid;

        @JsonProperty("order_type")
        public String orderType;

        public String tif;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ExpectedReport {
        @JsonProperty("exec_type")
        public String execType;

        @JsonProperty("order_status")
        public String orderStatus;

        @JsonProperty("client_oid")
        public String clientOid;

        @JsonProperty("filled_qty")
        public Long filledQty;

        @JsonProperty("leaves_qty")
        public Long leavesQty;

        @JsonProperty("cumulative_qty")
        public Long cumulativeQty;
    }

    // --- Exchange stubs ---

    static class DummyQueueModel implements QueueModel {
        @Override
        public void onModify(long prev, long next, ArrayDeque<group.gnometrading.backtest.book.LocalOrder> queue) {}
    }

    static class ZeroFeeModel implements FeeModel {
        @Override
        public double calculateFee(double totalPrice, boolean isMaker) {
            return 0;
        }
    }

    static class ZeroLatency implements LatencyModel {
        @Override
        public long simulate() {
            return 0;
        }
    }

    // --- Fixture loading ---

    static Stream<Arguments> loadScenarios() throws Exception {
        URL fixturesUrl = MbpExchangeScenarioTest.class.getClassLoader().getResource("fixtures");
        Path fixturesPath = Paths.get(fixturesUrl.toURI());
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return Files.list(fixturesPath)
                .filter(p -> p.toString().endsWith(".yaml"))
                .sorted()
                .map(p -> {
                    try {
                        Scenario scenario = mapper.readValue(p.toFile(), Scenario.class);
                        return Arguments.of(scenario.name, scenario);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    // --- Test ---

    @ParameterizedTest(name = "{0}")
    @MethodSource("loadScenarios")
    void testScenario(String name, Scenario scenario) {
        MbpSimulatedExchange exchange = new MbpSimulatedExchange(
                new ZeroFeeModel(), new ZeroLatency(), new ZeroLatency(), new DummyQueueModel());

        Map<String, Long> oidMap = new HashMap<>();
        long[] nextOid = {1L};

        if (scenario.initialLevels != null && !scenario.initialLevels.isEmpty()) {
            exchange.onMarketData(buildMarketUpdate(scenario.initialLevels));
        }

        for (Event event : scenario.events) {
            List<OrderExecutionReport> reports =
                    switch (event.type) {
                        case "submit_order" -> {
                            long oid = oidMap.computeIfAbsent(event.order.clientOid, k -> nextOid[0]++);
                            yield exchange.submitOrder(buildOrder(event.order, oid));
                        }
                        case "trade" -> exchange.onMarketData(
                                buildTrade(parseSide(event.side), event.price, event.size));
                        case "cancel_order" -> {
                            long oid = oidMap.computeIfAbsent(event.clientOid, k -> nextOid[0]++);
                            yield exchange.cancelOrder(buildCancel(oid));
                        }
                        case "modify_order" -> {
                            long oid = oidMap.computeIfAbsent(event.clientOid, k -> nextOid[0]++);
                            yield exchange.modifyOrder(buildModify(oid, event.newPrice, event.newSize));
                        }
                        case "market_update" -> exchange.onMarketData(buildMarketUpdate(event.levels));
                        default -> throw new IllegalArgumentException("Unknown event type: " + event.type);
                    };

            List<ExpectedReport> expected = event.expectedReports;
            assertEquals(
                    expected.size(),
                    reports.size(),
                    "Report count mismatch for event type=" + event.type + " in scenario=" + name);

            for (int i = 0; i < expected.size(); i++) {
                ExpectedReport exp = expected.get(i);
                OrderExecutionReport rep = reports.get(i);
                String ctx = "report[" + i + "] in event type=" + event.type + " in scenario=" + name;

                if (exp.execType != null) {
                    assertEquals(ExecType.valueOf(exp.execType), rep.decoder.execType(), "execType mismatch: " + ctx);
                }
                if (exp.orderStatus != null) {
                    assertEquals(
                            OrderStatus.valueOf(exp.orderStatus),
                            rep.decoder.orderStatus(),
                            "orderStatus mismatch: " + ctx);
                }
                if (exp.clientOid != null) {
                    assertEquals(
                            (long) oidMap.get(exp.clientOid), rep.getClientOidCounter(), "clientOid mismatch: " + ctx);
                }
                if (exp.filledQty != null) {
                    assertEquals((long) exp.filledQty, rep.decoder.filledQty(), "filledQty mismatch: " + ctx);
                }
                if (exp.leavesQty != null) {
                    assertEquals((long) exp.leavesQty, rep.decoder.leavesQty(), "leavesQty mismatch: " + ctx);
                }
                if (exp.cumulativeQty != null) {
                    assertEquals(
                            (long) exp.cumulativeQty, rep.decoder.cumulativeQty(), "cumulativeQty mismatch: " + ctx);
                }
            }
        }
    }

    // --- Schema builders ---

    private static Mbp10Schema buildMarketUpdate(List<Level> levels) {
        long[] bidPx = new long[10];
        long[] bidSz = new long[10];
        long[] askPx = new long[10];
        long[] askSz = new long[10];
        Arrays.fill(bidPx, PRICE_NULL);
        Arrays.fill(bidSz, SIZE_NULL);
        Arrays.fill(askPx, PRICE_NULL);
        Arrays.fill(askSz, SIZE_NULL);

        for (int i = 0; i < Math.min(levels.size(), 10); i++) {
            Level l = levels.get(i);
            if (l.bidPx != null) {
                bidPx[i] = l.bidPx;
                bidSz[i] = l.bidSz != null ? l.bidSz : SIZE_NULL;
            }
            if (l.askPx != null) {
                askPx[i] = l.askPx;
                askSz[i] = l.askSz != null ? l.askSz : SIZE_NULL;
            }
        }

        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Add);
        schema.encoder.side(Side.None);
        schema.encoder.price(PRICE_NULL);
        schema.encoder.size(SIZE_NULL);
        schema.encoder
                .bidPrice0(bidPx[0])
                .bidSize0(bidSz[0])
                .askPrice0(askPx[0])
                .askSize0(askSz[0]);
        schema.encoder
                .bidPrice1(bidPx[1])
                .bidSize1(bidSz[1])
                .askPrice1(askPx[1])
                .askSize1(askSz[1]);
        schema.encoder
                .bidPrice2(bidPx[2])
                .bidSize2(bidSz[2])
                .askPrice2(askPx[2])
                .askSize2(askSz[2]);
        schema.encoder
                .bidPrice3(bidPx[3])
                .bidSize3(bidSz[3])
                .askPrice3(askPx[3])
                .askSize3(askSz[3]);
        schema.encoder
                .bidPrice4(bidPx[4])
                .bidSize4(bidSz[4])
                .askPrice4(askPx[4])
                .askSize4(askSz[4]);
        schema.encoder
                .bidPrice5(bidPx[5])
                .bidSize5(bidSz[5])
                .askPrice5(askPx[5])
                .askSize5(askSz[5]);
        schema.encoder
                .bidPrice6(bidPx[6])
                .bidSize6(bidSz[6])
                .askPrice6(askPx[6])
                .askSize6(askSz[6]);
        schema.encoder
                .bidPrice7(bidPx[7])
                .bidSize7(bidSz[7])
                .askPrice7(askPx[7])
                .askSize7(askSz[7]);
        schema.encoder
                .bidPrice8(bidPx[8])
                .bidSize8(bidSz[8])
                .askPrice8(askPx[8])
                .askSize8(askSz[8]);
        schema.encoder
                .bidPrice9(bidPx[9])
                .bidSize9(bidSz[9])
                .askPrice9(askPx[9])
                .askSize9(askSz[9]);
        return schema;
    }

    private static Mbp10Schema buildTrade(Side side, long price, long size) {
        Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.action(Action.Trade);
        schema.encoder.side(side);
        schema.encoder.price(price);
        schema.encoder.size(size);
        schema.encoder
                .bidPrice0(PRICE_NULL)
                .bidSize0(SIZE_NULL)
                .askPrice0(PRICE_NULL)
                .askSize0(SIZE_NULL);
        schema.encoder
                .bidPrice1(PRICE_NULL)
                .bidSize1(SIZE_NULL)
                .askPrice1(PRICE_NULL)
                .askSize1(SIZE_NULL);
        schema.encoder
                .bidPrice2(PRICE_NULL)
                .bidSize2(SIZE_NULL)
                .askPrice2(PRICE_NULL)
                .askSize2(SIZE_NULL);
        schema.encoder
                .bidPrice3(PRICE_NULL)
                .bidSize3(SIZE_NULL)
                .askPrice3(PRICE_NULL)
                .askSize3(SIZE_NULL);
        schema.encoder
                .bidPrice4(PRICE_NULL)
                .bidSize4(SIZE_NULL)
                .askPrice4(PRICE_NULL)
                .askSize4(SIZE_NULL);
        schema.encoder
                .bidPrice5(PRICE_NULL)
                .bidSize5(SIZE_NULL)
                .askPrice5(PRICE_NULL)
                .askSize5(SIZE_NULL);
        schema.encoder
                .bidPrice6(PRICE_NULL)
                .bidSize6(SIZE_NULL)
                .askPrice6(PRICE_NULL)
                .askSize6(SIZE_NULL);
        schema.encoder
                .bidPrice7(PRICE_NULL)
                .bidSize7(SIZE_NULL)
                .askPrice7(PRICE_NULL)
                .askSize7(SIZE_NULL);
        schema.encoder
                .bidPrice8(PRICE_NULL)
                .bidSize8(SIZE_NULL)
                .askPrice8(PRICE_NULL)
                .askSize8(SIZE_NULL);
        schema.encoder
                .bidPrice9(PRICE_NULL)
                .bidSize9(SIZE_NULL)
                .askPrice9(PRICE_NULL)
                .askSize9(SIZE_NULL);
        return schema;
    }

    private static Order buildOrder(OrderSpec spec, long oid) {
        Order order = new Order();
        order.encoder
                .exchangeId((short) 1)
                .securityId(1)
                .price(spec.price)
                .size(spec.size)
                .side(parseSide(spec.side))
                .orderType(parseOrderType(spec.orderType))
                .timeInForce(parseTif(spec.tif));
        order.encodeClientOid(oid, 0);
        return order;
    }

    private static CancelOrder buildCancel(long oid) {
        CancelOrder cancel = new CancelOrder();
        cancel.encoder.exchangeId((short) 1).securityId(1);
        cancel.encodeClientOid(oid, 0);
        return cancel;
    }

    private static ModifyOrder buildModify(long oid, long newPrice, long newSize) {
        ModifyOrder modify = new ModifyOrder();
        modify.encoder.exchangeId((short) 1).securityId(1).price(newPrice).size(newSize);
        modify.encodeClientOid(oid, 0);
        return modify;
    }

    // --- Parsing helpers ---

    private static Side parseSide(String s) {
        return switch (s) {
            case "Bid" -> Side.Bid;
            case "Ask" -> Side.Ask;
            default -> throw new IllegalArgumentException("Unknown side: " + s);
        };
    }

    private static OrderType parseOrderType(String s) {
        return switch (s) {
            case "LIMIT" -> OrderType.LIMIT;
            case "MARKET" -> OrderType.MARKET;
            default -> throw new IllegalArgumentException("Unknown order type: " + s);
        };
    }

    private static TimeInForce parseTif(String s) {
        return switch (s) {
            case "GTC" -> TimeInForce.GOOD_TILL_CANCELED;
            case "IOC" -> TimeInForce.IMMEDIATE_OR_CANCELED;
            case "FOK" -> TimeInForce.FILL_OR_KILL;
            default -> throw new IllegalArgumentException("Unknown TIF: " + s);
        };
    }
}
