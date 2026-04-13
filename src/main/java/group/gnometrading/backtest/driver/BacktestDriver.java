package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.bridge.BytesSchemaFactory;
import group.gnometrading.backtest.exchange.SimulatedExchange;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sequencer.JournalReader;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.strategies.StrategyAgent;
import java.io.IOException;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Logger;
import org.agrona.concurrent.UnsafeBuffer;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Drives a backtest by replaying market data from S3 through the exchange simulation and strategy.
 *
 * <p>The driver simulates the market gateway layer: writing market data events to the strategy,
 * routing outbound orders to the simulated exchange, and delivering execution reports back.
 * The OMS always sits between the strategy and the exchange for intent resolution, risk
 * validation, and position tracking.
 *
 * <p>Event ordering: EXCHANGE_MARKET_DATA < EXCHANGE_MESSAGE < LOCAL_MARKET_DATA < LOCAL_MESSAGE
 * at the same timestamp, ensuring the exchange processes data before the strategy sees it.
 */
public final class BacktestDriver {

    private static final Logger logger = Logger.getLogger(BacktestDriver.class.getName());

    private final LocalDateTime startDate;
    private final LocalDateTime endDate;
    private final List<MarketDataEntry> entries;
    private final SchemaType schemaType;
    private final StrategyAgent strategy;
    // Map of exchangeId -> (securityId -> SimulatedExchange)
    private final Map<Integer, Map<Integer, SimulatedExchange>> exchanges;
    private final OmsBacktestAdapter adapter;
    private final S3Client s3Client;
    private final String bucket;

    // Drains intents published by the strategy after each doWork() call
    private final SequencedPoller intentPoller;
    private final List<Intent> pendingIntents = new ArrayList<>();

    private long lastProcessingTimeNs;
    private BacktestRecorder recorder;
    private PriorityQueue<BacktestEvent> queue;
    private boolean ready = false;
    private int eventsProcessed = 0;
    private int initialQueueSize = 0;
    private long lastTimestamp = 0;
    private long sequenceCounter = 0;

    public BacktestDriver(
            LocalDateTime startDate,
            LocalDateTime endDate,
            List<MarketDataEntry> entries,
            SchemaType schemaType,
            StrategyAgent strategy,
            Map<Integer, Map<Integer, SimulatedExchange>> exchanges,
            OmsBacktestAdapter adapter,
            S3Client s3Client,
            String bucket) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.entries = entries;
        this.schemaType = schemaType;
        this.strategy = strategy;
        this.exchanges = exchanges;
        this.adapter = adapter;
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.intentPoller = strategy.getIntentBuffer().createPoller(this::collectIntent);
    }

    private void collectIntent(long globalSeq, int templateId, UnsafeBuffer buf, int len) {
        if (templateId == IntentDecoder.TEMPLATE_ID) {
            Intent intent = new Intent();
            intent.buffer.putBytes(0, buf, 0, len);
            intent.wrap(intent.buffer);
            pendingIntents.add(intent);
        }
    }

    private List<Intent> drainIntents() throws Exception {
        pendingIntents.clear();
        intentPoller.poll();
        return pendingIntents;
    }

    private void stepStrategy() throws Exception {
        long t0 = System.nanoTime();
        strategy.doWork();
        lastProcessingTimeNs = System.nanoTime() - t0;
    }

    private long getProcessingTime() {
        long override = strategy.simulateProcessingTime();
        return override > 0 ? override : lastProcessingTimeNs;
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
                long localTs = schema.getEventTimestamp();
                queue.add(new BacktestEvent(exchangeTs, EventType.EXCHANGE_MARKET_DATA, schema));
                queue.add(new BacktestEvent(localTs, EventType.LOCAL_MARKET_DATA, schema));
            }
        }
        initialQueueSize = queue.size();
        ready = true;
    }

    /**
     * Loads market data from a production journal into the priority queue.
     * Each record produces two events: EXCHANGE_MARKET_DATA and LOCAL_MARKET_DATA.
     */
    public void prepareDataFromJournal(JournalReader reader) throws IOException {
        queue = new PriorityQueue<>();
        reader.readAll((globalSequence, templateId, buffer, length) -> {
            byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes);
            Schema schema = BytesSchemaFactory.fromBytes(bytes);
            long ts = schema.getEventTimestamp();
            queue.add(new BacktestEvent(ts, EventType.EXCHANGE_MARKET_DATA, schema));
            queue.add(new BacktestEvent(ts, EventType.LOCAL_MARKET_DATA, schema));
        });
        initialQueueSize = queue.size();
        ready = true;
    }

    public int getEventsProcessed() {
        return eventsProcessed;
    }

    public long getSequenceCounter() {
        return sequenceCounter;
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
                throw new RuntimeException(e);
            }
        }
    }

    private void processEvent(BacktestEvent event) throws Exception {
        sequenceCounter++;
        switch (event.eventType()) {
            case EXCHANGE_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                SimulatedExchange exchange = getExchange(schema);
                List<OrderExecutionReport> reports = exchange.onMarketData(schema);
                for (OrderExecutionReport report : reports) {
                    long deliveryTs = event.timestamp() + exchange.simulateNetworkLatency();
                    report.encoder.timestampEvent(event.timestamp()).timestampRecv(deliveryTs);
                    queue.add(new BacktestEvent(deliveryTs, EventType.EXCHANGE_MESSAGE, report));
                }
            }
            case EXCHANGE_MESSAGE -> {
                OrderExecutionReport report = (OrderExecutionReport) event.data();
                // OMS processes exec report first (position tracking, state updates, may emit orders)
                List<LocalMessage> omsMessages = adapter.processExecutionReport(report);
                // Deliver exec report to strategy and let it react
                strategy.submitExecReport(report);
                stepStrategy();
                List<Intent> strategyIntents = drainIntents();
                List<LocalMessage> strategyMessages = adapter.processIntents(event.timestamp(), strategyIntents);
                // OMS messages have no strategy processing delay; strategy messages do
                scheduleLocalMessages(event.timestamp(), omsMessages);
                scheduleLocalMessages(event.timestamp(), strategyMessages, getProcessingTime());
            }
            case LOCAL_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                if (recorder != null) {
                    recorder.onMarketData(event.timestamp(), schema);
                }
                strategy.submitMarketData(schema);
                stepStrategy();
                List<Intent> intents = drainIntents();
                List<LocalMessage> messages = adapter.processIntents(event.timestamp(), intents);
                scheduleLocalMessages(event.timestamp(), messages, getProcessingTime());
            }
            case LOCAL_MESSAGE -> {
                LocalMessage message = (LocalMessage) event.data();
                SimulatedExchange exchange = getExchangeForMessage(message);

                List<OrderExecutionReport> reports;
                if (message instanceof LocalMessage.OrderMessage om) {
                    reports = exchange.submitOrder(om.order());
                } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
                    reports = exchange.cancelOrder(cm.cancelOrder());
                } else if (message instanceof LocalMessage.ModifyOrderMessage am) {
                    reports = exchange.modifyOrder(am.modifyOrder());
                } else {
                    throw new IllegalStateException("Unknown local message type: " + message.getClass());
                }

                for (OrderExecutionReport report : reports) {
                    long deliveryTs = event.timestamp()
                            + exchange.simulateOrderProcessingTime()
                            + exchange.simulateNetworkLatency();
                    report.encoder.timestampEvent(event.timestamp()).timestampRecv(deliveryTs);
                    setExchangeIds(report, message);
                    queue.add(new BacktestEvent(deliveryTs, EventType.EXCHANGE_MESSAGE, report));
                }
            }
        }
    }

    private void scheduleLocalMessages(long eventTimestamp, List<LocalMessage> messages) {
        scheduleLocalMessages(eventTimestamp, messages, 0);
    }

    private void scheduleLocalMessages(long eventTimestamp, List<LocalMessage> messages, long processingTime) {
        for (LocalMessage message : messages) {
            SimulatedExchange exchange = getExchangeForMessage(message);
            long deliveryTs = eventTimestamp + processingTime + exchange.simulateNetworkLatency();
            queue.add(new BacktestEvent(deliveryTs, EventType.LOCAL_MESSAGE, message));
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
            Order order = om.order();
            return exchanges.get((int) order.decoder.exchangeId()).get((int) order.decoder.securityId());
        } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
            CancelOrder cancel = cm.cancelOrder();
            return exchanges.get((int) cancel.decoder.exchangeId()).get((int) cancel.decoder.securityId());
        } else if (message instanceof LocalMessage.ModifyOrderMessage mm) {
            ModifyOrder modify = mm.modifyOrder();
            return exchanges.get((int) modify.decoder.exchangeId()).get((int) modify.decoder.securityId());
        }
        throw new IllegalStateException("Unknown message type: " + message.getClass());
    }

    private void setExchangeIds(OrderExecutionReport report, LocalMessage message) {
        // exchangeId and securityId may already be set by the SimulatedExchange,
        // but for order submissions the exchange sets them from the order fields.
        // This is a no-op if already set — we only call it for clarification on the fill path.
        if (message instanceof LocalMessage.OrderMessage om) {
            Order order = om.order();
            report.encoder.exchangeId((short) order.decoder.exchangeId()).securityId(order.decoder.securityId());
        } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
            CancelOrder cancel = cm.cancelOrder();
            report.encoder.exchangeId((short) cancel.decoder.exchangeId()).securityId(cancel.decoder.securityId());
        } else if (message instanceof LocalMessage.ModifyOrderMessage mm) {
            ModifyOrder modify = mm.modifyOrder();
            report.encoder.exchangeId((short) modify.decoder.exchangeId()).securityId(modify.decoder.securityId());
        }
    }
}
