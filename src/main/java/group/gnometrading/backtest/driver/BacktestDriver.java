package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.backtest.exchange.SimulatedExchange;
import group.gnometrading.collector.MarketDataEntry;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Drives a backtest by replaying market data from S3 through the exchange simulation and strategy.
 *
 * Event ordering: EXCHANGE_MARKET_DATA < EXCHANGE_MESSAGE < LOCAL_MARKET_DATA < LOCAL_MESSAGE
 * at the same timestamp, ensuring the exchange processes data before the strategy sees it.
 */
public class BacktestDriver {

    private static final Logger logger = Logger.getLogger(BacktestDriver.class.getName());

    private final LocalDate startDate;
    private final LocalDate endDate;
    private final List<MarketDataEntry> entries;
    private final SchemaType schemaType;
    private final BacktestStrategy strategy;
    // Map of exchangeId -> (securityId -> SimulatedExchange)
    private final Map<Integer, Map<Integer, SimulatedExchange>> exchanges;
    private final S3Client s3Client;
    private final String bucket;

    private PriorityQueue<BacktestEvent> queue;
    private boolean ready = false;

    public BacktestDriver(
            LocalDate startDate,
            LocalDate endDate,
            List<MarketDataEntry> entries,
            SchemaType schemaType,
            BacktestStrategy strategy,
            Map<Integer, Map<Integer, SimulatedExchange>> exchanges,
            S3Client s3Client,
            String bucket) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.entries = entries;
        this.schemaType = schemaType;
        this.strategy = strategy;
        this.exchanges = exchanges;
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    /**
     * Loads all market data from S3 into the priority queue.
     * Each record produces two events: EXCHANGE_MARKET_DATA (at timestampEvent) and
     * LOCAL_MARKET_DATA (at timestampRecv).
     */
    public void prepareData() {
        queue = new PriorityQueue<>();
        for (MarketDataEntry entry : entries) {
            List<Schema> schemas = entry.loadFromS3(s3Client, bucket);
            for (Schema schema : schemas) {
                long exchangeTs = schema.getEventTimestamp();
                long localTs = schema.getEventTimestamp(); // timestampRecv is not on Schema base; use event timestamp
                queue.add(new BacktestEvent(exchangeTs, EventType.EXCHANGE_MARKET_DATA, schema));
                queue.add(new BacktestEvent(localTs, EventType.LOCAL_MARKET_DATA, schema));
            }
        }
        ready = true;
    }

    public void fullyExecute() {
        executeUntil(Long.MAX_VALUE);
    }

    public void executeUntil(long timestamp) {
        if (!ready) {
            throw new IllegalStateException("Call prepareData() before executing the backtest");
        }

        while (!queue.isEmpty()) {
            BacktestEvent event = queue.peek();
            if (event.timestamp >= timestamp) {
                break;
            }
            queue.poll();

            try {
                processEvent(event);
            } catch (Exception e) {
                logger.severe("Exception at timestamp " + event.timestamp + ": " + e.getMessage());
                throw e;
            }
        }
    }

    private void processEvent(BacktestEvent event) {
        switch (event.eventType) {
            case EXCHANGE_MARKET_DATA -> {
                Schema schema = (Schema) event.data;
                SimulatedExchange exchange = getExchange(schema);
                List<BacktestExecutionReport> reports = exchange.onMarketData(schema);
                for (BacktestExecutionReport report : reports) {
                    long deliveryTs = event.timestamp + exchange.simulateNetworkLatency();
                    queue.add(new BacktestEvent(deliveryTs, EventType.EXCHANGE_MESSAGE, report));
                }
            }
            case EXCHANGE_MESSAGE -> {
                BacktestExecutionReport report = (BacktestExecutionReport) event.data;
                strategy.onExecutionReport(event.timestamp, report);
            }
            case LOCAL_MARKET_DATA -> {
                Schema schema = (Schema) event.data;
                List<LocalMessage> messages = strategy.onMarketData(event.timestamp, schema);
                for (LocalMessage message : messages) {
                    SimulatedExchange exchange = getExchangeForMessage(message);
                    long deliveryTs = event.timestamp + strategy.simulateProcessingTime()
                            + exchange.simulateNetworkLatency();
                    queue.add(new BacktestEvent(deliveryTs, EventType.LOCAL_MESSAGE, message));
                }
            }
            case LOCAL_MESSAGE -> {
                LocalMessage message = (LocalMessage) event.data;
                SimulatedExchange exchange = getExchangeForMessage(message);

                List<BacktestExecutionReport> reports;
                if (message instanceof LocalMessage.OrderMessage om) {
                    reports = exchange.submitOrder(om.order());
                } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
                    reports = exchange.cancelOrder(cm.cancelOrder());
                } else {
                    throw new IllegalStateException("Unknown local message type: " + message.getClass());
                }

                for (BacktestExecutionReport report : reports) {
                    long deliveryTs = event.timestamp + exchange.simulateOrderProcessingTime()
                            + exchange.simulateNetworkLatency();
                    report.timestampEvent = event.timestamp;
                    report.timestampRecv = deliveryTs;
                    setExchangeIds(report, message);
                    queue.add(new BacktestEvent(deliveryTs, EventType.EXCHANGE_MESSAGE, report));
                }
            }
        }
    }

    private SimulatedExchange getExchange(Schema schema) {
        // Schema base class doesn't expose exchangeId/securityId directly; use schemaType discriminator
        // This requires subclasses or a helper — for now we use the first available exchange
        // TODO: extract exchangeId and securityId from the schema buffer directly
        return exchanges.values().iterator().next().values().iterator().next();
    }

    private SimulatedExchange getExchangeForMessage(LocalMessage message) {
        if (message instanceof LocalMessage.OrderMessage om) {
            BacktestOrder order = om.order();
            return exchanges.get(order.exchangeId()).get(order.securityId());
        } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
            BacktestCancelOrder cancel = cm.cancelOrder();
            return exchanges.get(cancel.exchangeId()).get(cancel.securityId());
        }
        throw new IllegalStateException("Unknown message type: " + message.getClass());
    }

    private void setExchangeIds(BacktestExecutionReport report, LocalMessage message) {
        if (message instanceof LocalMessage.OrderMessage om) {
            report.exchangeId = om.order().exchangeId();
            report.securityId = om.order().securityId();
        } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
            report.exchangeId = cm.cancelOrder().exchangeId();
            report.securityId = cm.cancelOrder().securityId();
        }
    }
}
