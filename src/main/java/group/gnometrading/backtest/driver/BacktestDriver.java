package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.exchange.BacktestAmendOrder;
import group.gnometrading.backtest.exchange.BacktestCancelOrder;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.backtest.exchange.BacktestOrder;
import group.gnometrading.backtest.exchange.SimulatedExchange;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Drives a backtest by replaying market data from S3 through the exchange simulation and strategy.
 *
 * Event ordering: EXCHANGE_MARKET_DATA < EXCHANGE_MESSAGE < LOCAL_MARKET_DATA < LOCAL_MESSAGE
 * at the same timestamp, ensuring the exchange processes data before the strategy sees it.
 */
public final class BacktestDriver {

    private static final Logger logger = Logger.getLogger(BacktestDriver.class.getName());

    private final LocalDateTime startDate;
    private final LocalDateTime endDate;
    private final List<MarketDataEntry> entries;
    private final SchemaType schemaType;
    private final BacktestStrategy strategy;
    // Map of exchangeId -> (securityId -> SimulatedExchange)
    private final Map<Integer, Map<Integer, SimulatedExchange>> exchanges;
    private final S3Client s3Client;
    private final String bucket;

    private BacktestRecorder recorder;
    private PriorityQueue<BacktestEvent> queue;
    private boolean ready = false;
    private int eventsProcessed = 0;
    private int initialQueueSize = 0;
    private long lastTimestamp = 0;

    public BacktestDriver(
            LocalDateTime startDate,
            LocalDateTime endDate,
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

    public void setRecorder(BacktestRecorder recorder) {
        this.recorder = recorder;
    }

    public BacktestRecorder getRecorder() {
        return this.recorder;
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
        initialQueueSize = queue.size();
        ready = true;
    }

    public int getEventsProcessed() {
        return eventsProcessed;
    }

    public int getInitialQueueSize() {
        return initialQueueSize;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
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
            if (event.timestamp() >= timestamp) {
                break;
            }
            queue.poll();
            eventsProcessed++;
            lastTimestamp = event.timestamp();

            try {
                processEvent(event);
            } catch (Exception e) {
                logger.severe("Exception at timestamp " + event.timestamp() + ": " + e.getMessage());
                throw e;
            }
        }
    }

    private void processEvent(BacktestEvent event) {
        switch (event.eventType()) {
            case EXCHANGE_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                SimulatedExchange exchange = getExchange(schema);
                List<BacktestExecutionReport> reports = exchange.onMarketData(schema);
                for (BacktestExecutionReport report : reports) {
                    long deliveryTs = event.timestamp() + exchange.simulateNetworkLatency();
                    report.timestampEvent = event.timestamp();
                    report.timestampRecv = deliveryTs;
                    queue.add(new BacktestEvent(deliveryTs, EventType.EXCHANGE_MESSAGE, report));
                }
            }
            case EXCHANGE_MESSAGE -> {
                BacktestExecutionReport report = (BacktestExecutionReport) event.data();
                List<LocalMessage> messages = strategy.onExecutionReport(event.timestamp(), report);
                if (messages != null) {
                    for (LocalMessage message : messages) {
                        SimulatedExchange exchange = getExchangeForMessage(message);
                        long deliveryTs = event.timestamp() + exchange.simulateNetworkLatency();
                        queue.add(new BacktestEvent(deliveryTs, EventType.LOCAL_MESSAGE, message));
                    }
                }
            }
            case LOCAL_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                if (recorder != null) {
                    recorder.onMarketData(event.timestamp(), schema);
                }
                List<LocalMessage> messages = strategy.onMarketData(event.timestamp(), schema);
                for (LocalMessage message : messages) {
                    SimulatedExchange exchange = getExchangeForMessage(message);
                    long deliveryTs =
                            event.timestamp() + strategy.simulateProcessingTime() + exchange.simulateNetworkLatency();
                    queue.add(new BacktestEvent(deliveryTs, EventType.LOCAL_MESSAGE, message));
                }
            }
            case LOCAL_MESSAGE -> {
                LocalMessage message = (LocalMessage) event.data();
                SimulatedExchange exchange = getExchangeForMessage(message);

                List<BacktestExecutionReport> reports;
                if (message instanceof LocalMessage.OrderMessage om) {
                    reports = exchange.submitOrder(om.order());
                } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
                    reports = exchange.cancelOrder(cm.cancelOrder());
                } else if (message instanceof LocalMessage.AmendOrderMessage am) {
                    reports = exchange.amendOrder(am.amendOrder());
                } else {
                    throw new IllegalStateException("Unknown local message type: " + message.getClass());
                }

                for (BacktestExecutionReport report : reports) {
                    long deliveryTs = event.timestamp()
                            + exchange.simulateOrderProcessingTime()
                            + exchange.simulateNetworkLatency();
                    report.timestampEvent = event.timestamp();
                    report.timestampRecv = deliveryTs;
                    setExchangeIds(report, message);
                    queue.add(new BacktestEvent(deliveryTs, EventType.EXCHANGE_MESSAGE, report));
                }
            }
        }
    }

    private SimulatedExchange getExchange(Schema schema) {
        // exchangeId and securityId are at consistent offsets across all schema types:
        // after the 8-byte SBE message header, exchangeId is a 2-byte uint16 at offset 0,
        // and securityId is a 4-byte uint32 at offset 2.
        int headerSize = MessageHeaderDecoder.ENCODED_LENGTH;
        int exchangeId = schema.buffer.getShort(headerSize, ByteOrder.LITTLE_ENDIAN);
        int securityId = schema.buffer.getInt(headerSize + 2, ByteOrder.LITTLE_ENDIAN);
        return exchanges.get(exchangeId).get(securityId);
    }

    private SimulatedExchange getExchangeForMessage(LocalMessage message) {
        if (message instanceof LocalMessage.OrderMessage om) {
            BacktestOrder order = om.order();
            return exchanges.get(order.exchangeId()).get(order.securityId());
        } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
            BacktestCancelOrder cancel = cm.cancelOrder();
            return exchanges.get(cancel.exchangeId()).get(cancel.securityId());
        } else if (message instanceof LocalMessage.AmendOrderMessage am) {
            BacktestAmendOrder amend = am.amendOrder();
            return exchanges.get(amend.exchangeId()).get(amend.securityId());
        }
        throw new IllegalStateException("Unknown message type: " + message.getClass());
    }

    private void setExchangeIds(BacktestExecutionReport report, LocalMessage message) {
        if (message instanceof LocalMessage.OrderMessage om) {
            report.exchangeId = om.order().exchangeId();
            report.securityId = om.order().securityId();
            report.side = om.order().side();
        } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
            report.exchangeId = cm.cancelOrder().exchangeId();
            report.securityId = cm.cancelOrder().securityId();
        } else if (message instanceof LocalMessage.AmendOrderMessage am) {
            report.exchangeId = am.amendOrder().exchangeId();
            report.securityId = am.amendOrder().securityId();
        }
    }
}
