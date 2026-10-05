package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.bridge.BytesSchemaFactory;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.backtest.recorder.MetricAware;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.oms.pnl.PriceWriterAgent;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.JournalReader;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.strategies.StrategyAgent;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.agrona.concurrent.UnsafeBuffer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

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
    // Caps the re-steps at one timestamp for a strategy that answers every reject with another rejected intent.
    private static final int MAX_REJECT_PASSES = 16;

    private final List<MarketDataEntry> entries;
    private final StrategyAgent strategy;
    // Map of exchangeId -> (securityId -> SimulatedExchange)
    private final Map<Integer, Map<Integer, SimulatedExchange>> exchanges;
    private final OmsBacktestAdapter adapter;
    private final PriceWriterAgent priceWriter;
    private final S3Client s3Client;
    private final String bucket;
    private final boolean measureProcessingTime;

    // Drains intents published by the strategy after each doWork() call
    private final SequencedPoller intentPoller;
    private final List<Intent> pendingIntents = new ArrayList<>();

    private long lastProcessingTimeNs;
    private BacktestRecorder recorder;
    private PriorityQueue<BacktestEvent> queue;
    private long nextSequence = 0;
    private boolean ready = false;
    private int eventsProcessed = 0;
    private boolean warnedRejectLoop = false;

    public BacktestDriver(
            List<MarketDataEntry> entries,
            StrategyAgent strategy,
            Map<Integer, Map<Integer, SimulatedExchange>> exchanges,
            OmsBacktestAdapter adapter,
            PriceWriterAgent priceWriter,
            S3Client s3Client,
            String bucket,
            BacktestRecorder recorder,
            boolean measureProcessingTime) {
        this.entries = entries;
        this.strategy = strategy;
        this.exchanges = exchanges;
        this.adapter = adapter;
        this.priceWriter = priceWriter;
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.recorder = recorder;
        this.measureProcessingTime = measureProcessingTime;
        this.intentPoller = strategy.getIntentBuffer().createPoller(this::collectIntent);
        if (recorder != null && strategy instanceof MetricAware metricAware) {
            metricAware.setMetricRecorder(recorder.createMetricRecorder());
        }
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
        if (!measureProcessingTime) {
            strategy.doWork();
            return;
        }
        long t0 = System.nanoTime();
        strategy.doWork();
        lastProcessingTimeNs = System.nanoTime() - t0;
    }

    private long getProcessingTime() {
        if (measureProcessingTime) {
            return lastProcessingTimeNs;
        }
        long processingTime = strategy.simulateProcessingTime();
        if (processingTime < 0) {
            throw new IllegalStateException("simulateProcessingTime() returned a negative value: " + processingTime);
        }
        return processingTime;
    }

    /**
     * Loads all market data from S3 into the priority queue in parallel.
     * Each record produces two events: EXCHANGE_MARKET_DATA at the event timestamp and
     * LOCAL_MARKET_DATA one simulated network hop later. Missing keys are skipped and recorded,
     * as are records carrying a null event timestamp.
     */
    public void prepareData() {
        queue = new PriorityQueue<>();
        int total = entries.size();
        int logInterval = Math.max(1, total / 10);
        AtomicInteger completed = new AtomicInteger(0);
        AtomicInteger missingCount = new AtomicInteger(0);

        int droppedCount = 0;

        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<List<Schema>>> futures = new ArrayList<>(total);

        for (MarketDataEntry entry : entries) {
            futures.add(pool.submit(() -> {
                try {
                    return entry.loadFromS3(s3Client, bucket);
                } catch (NoSuchKeyException e) {
                    logger.warning("Missing S3 key: " + entry.getKey());
                    missingCount.incrementAndGet();
                    return Collections.emptyList();
                } finally {
                    int done = completed.incrementAndGet();
                    if (done % logInterval == 0 || done == total) {
                        logger.info(String.format(
                                "prepareData: %d/%d (%d%%) loaded, %d missing",
                                done, total, done * 100 / total, missingCount.get()));
                    }
                }
            }));
        }

        pool.shutdown();
        try {
            for (Future<List<Schema>> future : futures) {
                for (Schema schema : future.get()) {
                    long exchangeTs = schema.getEventTimestamp();
                    if (exchangeTs <= 0) {
                        logger.warning("Dropping " + schema.schemaType + " record with null event timestamp");
                        droppedCount++;
                        continue;
                    }
                    long localTs = exchangeTs + getExchange(schema).simulateNetworkLatency();
                    enqueue(exchangeTs, EventType.EXCHANGE_MARKET_DATA, schema);
                    enqueue(localTs, EventType.LOCAL_MARKET_DATA, schema);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Data loading interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Data loading failed", e.getCause());
        }

        logger.info(String.format(
                "prepareData: complete — %d entries, %d missing, %d dropped (null event timestamp)",
                total, missingCount.get(), droppedCount));
        ready = true;
    }

    /**
     * Loads market data from a production journal into the priority queue.
     * Each record produces two events: EXCHANGE_MARKET_DATA and LOCAL_MARKET_DATA.
     */
    public void prepareDataFromJournal(JournalReader reader) throws IOException {
        queue = new PriorityQueue<>();
        int[] dropped = new int[1];
        reader.readAll((globalSequence, templateId, buffer, length) -> {
            byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes);
            Schema schema = BytesSchemaFactory.fromBytes(bytes);
            long exchangeTs = schema.getEventTimestamp();
            if (exchangeTs <= 0) {
                logger.warning("Dropping " + schema.schemaType + " record with null event timestamp at global sequence "
                        + globalSequence);
                dropped[0]++;
                return;
            }
            enqueue(exchangeTs, EventType.EXCHANGE_MARKET_DATA, schema);
            long localTs = exchangeTs + getExchange(schema).simulateNetworkLatency();
            enqueue(localTs, EventType.LOCAL_MARKET_DATA, schema);
        });
        if (dropped[0] > 0) {
            logger.warning(String.format(
                    "prepareDataFromJournal: dropped %d record(s) with null event timestamp", dropped[0]));
        }
        ready = true;
    }

    public int getEventsProcessed() {
        return eventsProcessed;
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

            try {
                processEvent(event);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception at timestamp " + event.timestamp(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void processEvent(BacktestEvent event) throws Exception {
        switch (event.eventType()) {
            case EXCHANGE_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                SimulatedExchange exchange = getExchange(schema);
                List<OrderExecutionReport> reports = exchange.onMarketData(schema);
                for (OrderExecutionReport report : reports) {
                    long deliveryTs = event.timestamp() + exchange.simulateNetworkLatency();
                    report.encoder.timestampEvent(event.timestamp()).timestampRecv(deliveryTs);
                    enqueue(deliveryTs, EventType.EXCHANGE_MESSAGE, report);
                }
            }
            case EXCHANGE_MESSAGE -> {
                OrderExecutionReport report = (OrderExecutionReport) event.data();
                // OMS processes exec report first (position tracking, state updates, may emit orders)
                List<LocalMessage> omsMessages = adapter.processExecutionReport(report);
                // Schedule OMS messages before calling processIntents — both share the same internal
                // messageBuffer, so processIntents.clear() would destroy omsMessages if we waited.
                scheduleLocalMessages(event.timestamp(), omsMessages);
                // OMS forwards exec report to strategy (after position state is updated)
                for (OrderExecutionReport forwarded : adapter.getStrategyExecReports()) {
                    strategy.submitExecReport(forwarded);
                }
                runStrategy(event.timestamp());
            }
            case LOCAL_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                if (recorder != null) {
                    recorder.onMarketData(event.timestamp(), schema);
                }
                strategy.submitMarketData(schema);
                // Before the strategy acts, so the OMS values its intents against the same book it saw.
                priceWriter.doWork();
                runStrategy(event.timestamp());
            }
            case LOCAL_MESSAGE -> {
                LocalMessage message = (LocalMessage) event.data();
                SimulatedExchange exchange = getExchangeForMessage(message);

                List<OrderExecutionReport> reports;
                boolean isMaker = true;
                if (message instanceof LocalMessage.OrderMessage om) {
                    reports = exchange.submitOrder(om.order());
                    isMaker = reports.stream()
                            .noneMatch(r -> r.decoder.execType() == ExecType.FILL
                                    || r.decoder.execType() == ExecType.PARTIAL_FILL);
                } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
                    reports = exchange.cancelOrder(cm.cancelOrder());
                } else if (message instanceof LocalMessage.ModifyOrderMessage am) {
                    reports = exchange.modifyOrder(am.modifyOrder());
                } else {
                    throw new IllegalStateException("Unknown local message type: " + message.getClass());
                }

                long processingTime = exchange.simulateOrderProcessingTime(isMaker);
                for (OrderExecutionReport report : reports) {
                    long deliveryTs = event.timestamp() + processingTime + exchange.simulateNetworkLatency();
                    report.encoder.timestampEvent(event.timestamp()).timestampRecv(deliveryTs);
                    setExchangeIds(report, message);
                    enqueue(deliveryTs, EventType.EXCHANGE_MESSAGE, report);
                }
            }
        }
    }

    /**
     * Steps the strategy and routes its intents through the OMS. Risk rejects come back from the OMS at once, as they
     * do live, so the strategy is stepped again at the same simulated time to react to them rather than at whatever
     * event comes next.
     */
    private void runStrategy(long timestamp) throws Exception {
        stepStrategy();
        for (int pass = 1; ; pass++) {
            List<LocalMessage> messages = adapter.processIntents(timestamp, drainIntents());
            // Scheduled before the next processIntents call, which reuses the adapter's message list.
            scheduleLocalMessages(timestamp, messages, getProcessingTime());
            List<OrderExecutionReport> rejects = adapter.getStrategyExecReports();
            if (rejects.isEmpty()) {
                return;
            }
            for (OrderExecutionReport reject : rejects) {
                strategy.submitExecReport(reject);
            }
            if (pass == MAX_REJECT_PASSES) {
                // The remaining rejects reach the strategy on its next event. Warned once: a strategy stuck in this
                // loop would otherwise log it on every tick.
                if (!warnedRejectLoop) {
                    warnedRejectLoop = true;
                    logger.warning("Strategy still sending rejected intents after " + MAX_REJECT_PASSES
                            + " passes at timestamp " + timestamp + "; further occurrences are not logged");
                }
                return;
            }
            stepStrategy();
        }
    }

    private void scheduleLocalMessages(long eventTimestamp, List<LocalMessage> messages) {
        scheduleLocalMessages(eventTimestamp, messages, 0);
    }

    private void scheduleLocalMessages(long eventTimestamp, List<LocalMessage> messages, long processingTime) {
        for (LocalMessage message : messages) {
            SimulatedExchange exchange = getExchangeForMessage(message);
            long deliveryTs = eventTimestamp + processingTime + exchange.simulateNetworkLatency();
            enqueue(deliveryTs, EventType.LOCAL_MESSAGE, message);
            if (recorder != null && message instanceof LocalMessage.OrderMessage om) {
                recorder.onOrderSubmitted(eventTimestamp, om.order());
            }
        }
    }

    private void enqueue(long timestamp, EventType eventType, Object data) {
        queue.add(new BacktestEvent(timestamp, eventType, nextSequence++, data));
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
        return exchanges.get(message.exchangeId()).get(message.securityId());
    }

    private void setExchangeIds(OrderExecutionReport report, LocalMessage message) {
        report.encoder.exchangeId((short) message.exchangeId()).securityId(message.securityId());
    }
}
