package group.gnometrading.backtest.driver;

import group.gnometrading.backtest.bridge.BytesSchemaFactory;
import group.gnometrading.backtest.oms.OmsBacktestAdapter;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.backtest.recorder.MetricAware;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.oms.pnl.PriceWriterAgent;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.JournalReader;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.simulation.exchange.SimulatedExchange;
import group.gnometrading.strategies.StrategyAgent;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;
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
 * <p>Event ordering at the same timestamp: EXCHANGE_MARKET_DATA < EXCHANGE_PROCESS < EXCHANGE_MESSAGE <
 * LOCAL_MARKET_DATA < LOCAL_MESSAGE < STRATEGY_DONE, so the exchange processes data before the strategy sees it.
 *
 * <p>Each listing has three connections, as live: market data in, orders out, and exec reports in. Market data
 * arrives when its {@link MarketDataArrival} says, by default the receive time recorded with it; orders and reports
 * each draw the profile's network latency. Like TCP, a connection delivers in send order, so a slow message holds up
 * those behind it. Market data and exec reports travel on different connections and can still overtake each other.
 *
 * <p>The strategy handles one event at a time. An event takes its processing time, after which its intents reach
 * the OMS; events arriving meanwhile wait their turn in arrival order. The OMS and the price writer run on their own
 * threads live, so they see every message as soon as it arrives.
 */
public final class BacktestDriver {

    private static final Logger logger = Logger.getLogger(BacktestDriver.class.getName());
    // Caps the steps in a row that only answer rejects, for a strategy that answers every reject with another rejected
    // intent. With processing time each step moves time on, so without it such a strategy would never let the run end.
    private static final int MAX_REJECT_PASSES = 16;
    private static final int LOADER_THREADS = 16;
    // Enough to keep the loader threads busy ahead of the run while holding only half an hour of data in memory.
    private static final int MINUTES_AHEAD = 30;
    private static final long LOAD_SLACK_NANOS = 60_000_000_000L;

    private final List<MarketDataEntry> entries;
    private final Map<SimulatedExchange, Links> links = new IdentityHashMap<>();
    // What has reached the strategy but not been handled yet, one entry per arrival.
    private final ArrayDeque<Arrival> strategyInbox = new ArrayDeque<>();
    private final StrategyAgent strategy;
    // Map of exchangeId -> (securityId -> SimulatedExchange)
    private final Map<Integer, Map<Integer, SimulatedExchange>> exchanges;
    private final OmsBacktestAdapter adapter;
    private final PriceWriterAgent priceWriter;
    // The price writer's own copy of the market data, written on arrival rather than when the strategy is free.
    private final SequencedRingBuffer<?> priceFeed;
    private final S3Client s3Client;
    private final String bucket;
    private final boolean measureProcessingTime;

    // Drains intents published by the strategy after each doWork() call
    private final SequencedPoller intentPoller;
    private final List<Intent> pendingIntents = new ArrayList<>();

    private BacktestRecorder recorder;
    private PriorityQueue<BacktestEvent> queue;
    private MarketDataStream stream;
    private long lastProcessedTimestamp = Long.MIN_VALUE;
    private int droppedNullTimestamp = 0;
    private int droppedLate = 0;
    private long nextSequence = 0;
    private boolean ready = false;
    private int eventsProcessed = 0;
    private boolean warnedRejectLoop = false;
    private boolean strategyBusy = false;
    private int rejectSteps = 0;

    /** The connections between us and one listing's venue. */
    private record Links(
            MarketDataArrival marketDataArrival,
            NetworkChannel marketData,
            NetworkChannel orders,
            NetworkChannel reports) {}

    /** Messages that reached the strategy together; {@code rejects} marks the OMS refusing its own intents. */
    private record Arrival(List<?> messages, boolean rejects) {}

    public BacktestDriver(
            List<MarketDataEntry> entries,
            StrategyAgent strategy,
            Map<Integer, Map<Integer, SimulatedExchange>> exchanges,
            Map<SimulatedExchange, MarketDataArrival> marketDataArrivals,
            OmsBacktestAdapter adapter,
            PriceWriterAgent priceWriter,
            SequencedRingBuffer<?> priceFeed,
            S3Client s3Client,
            String bucket,
            BacktestRecorder recorder,
            boolean measureProcessingTime) {
        this.entries = entries;
        this.strategy = strategy;
        this.exchanges = exchanges;
        this.adapter = adapter;
        this.priceWriter = priceWriter;
        this.priceFeed = priceFeed;
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.recorder = recorder;
        this.measureProcessingTime = measureProcessingTime;
        this.intentPoller = strategy.getIntentBuffer().createPoller(this::collectIntent);
        for (Map<Integer, SimulatedExchange> bySecurity : exchanges.values()) {
            for (SimulatedExchange exchange : bySecurity.values()) {
                MarketDataArrival arrival = marketDataArrivals.get(exchange);
                if (arrival == null) {
                    throw new IllegalArgumentException("No market data arrival given for an exchange");
                }
                links.put(
                        exchange, new Links(arrival, new NetworkChannel(), new NetworkChannel(), new NetworkChannel()));
            }
        }
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

    /** Runs the strategy on what it has been given and returns how long that took in simulated time. */
    private long runStrategyOnce() throws Exception {
        if (measureProcessingTime) {
            long t0 = System.nanoTime();
            strategy.doWork();
            return System.nanoTime() - t0;
        }
        strategy.doWork();
        long processingTime = strategy.simulateProcessingTime();
        if (processingTime < 0) {
            throw new IllegalStateException("simulateProcessingTime() returned a negative value: " + processingTime);
        }
        return processingTime;
    }

    /**
     * Readies market data to stream from S3 a minute at a time as the run reaches it, downloading ahead on background
     * threads. Each record becomes one EXCHANGE_MARKET_DATA event at its event timestamp; its LOCAL_MARKET_DATA copy
     * is scheduled when the exchange processes it, so the market data connection carries records in order.
     */
    public void prepareData() {
        queue = new PriorityQueue<>();
        stream = new MarketDataStream(entries, s3Client, bucket, LOADER_THREADS, MINUTES_AHEAD);
        ready = true;
    }

    /** Loads records already in memory, in the order given; for tests. */
    void prepareRecords(List<? extends Schema> records) {
        queue = new PriorityQueue<>();
        for (Schema schema : records) {
            enqueueRecord(schema);
        }
        ready = true;
    }

    /**
     * Queues every minute of market data the next event could need. A file holds one minute of event time, so once
     * the minutes starting up to a minute past the earliest queued event are in, every record due before it is
     * queued, along with any filed a minute late.
     */
    private void loadMarketData() {
        while (stream != null
                && stream.hasNext()
                && (queue.isEmpty()
                        || stream.nextMinuteStartNanos() <= queue.peek().timestamp() + LOAD_SLACK_NANOS)) {
            for (Schema schema : stream.next()) {
                enqueueRecord(schema);
            }
            if (!stream.hasNext()) {
                logger.info(String.format(
                        "Market data: %d files read, %d empty minutes, %d records dropped without an event"
                                + " timestamp, %d dropped as late",
                        stream.files(), stream.missing(), droppedNullTimestamp, droppedLate));
            }
        }
    }

    /** Queues a record for the exchange at its event timestamp, unless it has none or the run is already past it. */
    private void enqueueRecord(Schema schema) {
        long exchangeTs = schema.getEventTimestamp();
        if (exchangeTs <= 0) {
            logger.warning("Dropping " + schema.schemaType + " record with null event timestamp");
            droppedNullTimestamp++;
            return;
        }
        if (exchangeTs < lastProcessedTimestamp) {
            // Filed more than a minute away from its event time; replaying it now would run time backwards.
            if (droppedLate == 0) {
                logger.warning("Dropping " + schema.schemaType + " record at " + exchangeTs
                        + ", filed after the run passed its time; further ones are only counted");
            }
            droppedLate++;
            return;
        }
        enqueue(exchangeTs, EventType.EXCHANGE_MARKET_DATA, schema);
    }

    /**
     * Loads market data from a production journal into the priority queue, one EXCHANGE_MARKET_DATA event per
     * record; see {@link #prepareData()}.
     */
    public void prepareDataFromJournal(JournalReader reader) throws IOException {
        queue = new PriorityQueue<>();
        reader.readAll((globalSequence, templateId, buffer, length) -> {
            byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes);
            enqueueRecord(BytesSchemaFactory.fromBytes(bytes));
        });
        if (droppedNullTimestamp > 0) {
            logger.warning(String.format(
                    "prepareDataFromJournal: dropped %d record(s) with null event timestamp", droppedNullTimestamp));
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

        while (true) {
            loadMarketData();
            BacktestEvent event = queue.peek();
            if (event == null || event.timestamp() >= timestamp) {
                break;
            }
            queue.poll();
            eventsProcessed++;
            lastProcessedTimestamp = event.timestamp();

            try {
                processEvent(event);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception at timestamp " + event.timestamp(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void processEvent(BacktestEvent event) throws Exception {
        long timestamp = event.timestamp();
        switch (event.eventType()) {
            case EXCHANGE_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                SimulatedExchange exchange = getExchange(schema);
                Links link = links.get(exchange);
                long received = link.marketDataArrival().arrival(schema, timestamp);
                enqueue(link.marketData().arrival(received, 0), EventType.LOCAL_MARKET_DATA, schema);
                deliverReports(exchange, timestamp, exchange.onMarketData(schema));
            }
            case EXCHANGE_MESSAGE -> {
                OrderExecutionReport report = (OrderExecutionReport) event.data();
                // The OMS updates positions and order state first, and may send orders of its own.
                sendToExchanges(timestamp, adapter.processExecutionReport(report));
                List<OrderExecutionReport> forwarded = adapter.getStrategyExecReports();
                if (!forwarded.isEmpty()) {
                    // A copy: the adapter reuses its list.
                    deliverToStrategy(timestamp, new Arrival(new ArrayList<>(forwarded), false));
                }
            }
            case LOCAL_MARKET_DATA -> {
                Schema schema = (Schema) event.data();
                if (recorder != null) {
                    recorder.onMarketData(timestamp, schema);
                }
                priceFeed.publishRaw(
                        schema.buffer, schema.messageHeaderDecoder.templateId(), schema.totalMessageSize());
                priceWriter.doWork();
                deliverToStrategy(timestamp, new Arrival(List.of(schema), false));
            }
            case LOCAL_MESSAGE -> {
                LocalMessage message = (LocalMessage) event.data();
                SimulatedExchange exchange = getExchangeForMessage(message);
                List<OrderExecutionReport> rejects;
                if (message instanceof LocalMessage.OrderMessage om) {
                    rejects = exchange.submitOrder(om.order(), timestamp);
                } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
                    rejects = exchange.cancelOrder(cm.cancelOrder(), timestamp);
                } else if (message instanceof LocalMessage.ModifyOrderMessage am) {
                    rejects = exchange.modifyOrder(am.modifyOrder(), timestamp);
                } else {
                    throw new IllegalStateException("Unknown local message type: " + message.getClass());
                }
                deliverReports(exchange, timestamp, rejects);
                scheduleExchangeProcessing(exchange);
            }
            case EXCHANGE_PROCESS -> {
                SimulatedExchange exchange = (SimulatedExchange) event.data();
                deliverReports(exchange, timestamp, exchange.processDue(timestamp));
                scheduleExchangeProcessing(exchange);
            }
            case STRATEGY_DONE -> {
                @SuppressWarnings("unchecked")
                List<Intent> intents = (List<Intent>) event.data();
                strategyBusy = false;
                routeIntents(timestamp, intents);
                stepStrategy(timestamp);
            }
        }
    }

    /** Sends exchange reports made at {@code eventTimestamp} back to the OMS over the listing's report connection. */
    private void deliverReports(SimulatedExchange exchange, long eventTimestamp, List<OrderExecutionReport> reports) {
        NetworkChannel channel = links.get(exchange).reports();
        for (OrderExecutionReport report : reports) {
            long deliveryTs = channel.arrival(eventTimestamp, exchange.simulateNetworkLatency());
            report.encoder.timestampEvent(eventTimestamp).timestampRecv(deliveryTs);
            enqueue(deliveryTs, EventType.EXCHANGE_MESSAGE, report);
        }
    }

    /** Wakes the exchange when the next message it holds comes due. A wakeup with nothing due is harmless. */
    private void scheduleExchangeProcessing(SimulatedExchange exchange) {
        long due = exchange.nextDueNanos();
        if (due != Long.MAX_VALUE) {
            enqueue(due, EventType.EXCHANGE_PROCESS, exchange);
        }
    }

    /** Hands the strategy what just reached it, or queues it behind what it is still working through. */
    private void deliverToStrategy(long timestamp, Arrival arrival) throws Exception {
        strategyInbox.add(arrival);
        if (!strategyBusy) {
            stepStrategy(timestamp);
        }
    }

    /**
     * Handles waiting arrivals one at a time. One that takes processing time keeps the strategy busy until
     * STRATEGY_DONE, when its intents reach the OMS; one that takes none is finished at once.
     */
    private void stepStrategy(long timestamp) throws Exception {
        while (!strategyInbox.isEmpty()) {
            Arrival arrival = strategyInbox.poll();
            if (!arrival.rejects()) {
                rejectSteps = 0;
            }
            for (Object message : arrival.messages()) {
                if (message instanceof Schema schema) {
                    strategy.submitMarketData(schema);
                } else {
                    strategy.submitExecReport((OrderExecutionReport) message);
                }
            }
            long processingTime = runStrategyOnce();
            if (processingTime > 0) {
                strategyBusy = true;
                // A copy: the next drain reuses the list.
                enqueue(timestamp + processingTime, EventType.STRATEGY_DONE, new ArrayList<>(drainIntents()));
                return;
            }
            routeIntents(timestamp, drainIntents());
        }
    }

    /**
     * Passes intents to the OMS and sends its orders on. Risk rejects come back from the OMS at once, as they do live,
     * and reach the strategy as their own event.
     */
    private void routeIntents(long timestamp, List<Intent> intents) throws Exception {
        sendToExchanges(timestamp, adapter.processIntents(timestamp, intents));
        List<OrderExecutionReport> rejects = adapter.getStrategyExecReports();
        if (rejects.isEmpty()) {
            return;
        }
        if (++rejectSteps < MAX_REJECT_PASSES) {
            strategyInbox.add(new Arrival(new ArrayList<>(rejects), true));
            return;
        }
        // The remaining rejects reach the strategy with its next event. Warned once: a strategy stuck in this loop
        // would otherwise log it on every tick.
        for (OrderExecutionReport reject : rejects) {
            strategy.submitExecReport(reject);
        }
        if (!warnedRejectLoop) {
            warnedRejectLoop = true;
            logger.warning("Strategy still sending rejected intents after " + MAX_REJECT_PASSES
                    + " passes in a row at timestamp " + timestamp + "; further occurrences are not logged");
        }
    }

    /** Sends messages leaving at {@code sentTimestamp} over each one's order connection. */
    private void sendToExchanges(long sentTimestamp, List<LocalMessage> messages) {
        for (LocalMessage message : messages) {
            SimulatedExchange exchange = getExchangeForMessage(message);
            long deliveryTs = links.get(exchange).orders().arrival(sentTimestamp, exchange.simulateNetworkLatency());
            enqueue(deliveryTs, EventType.LOCAL_MESSAGE, message);
            if (recorder != null) {
                if (message instanceof LocalMessage.OrderMessage om) {
                    recorder.onOrderSubmitted(sentTimestamp, om.order());
                } else if (message instanceof LocalMessage.ModifyOrderMessage mm) {
                    recorder.onModifySubmitted(mm.modifyOrder());
                } else if (message instanceof LocalMessage.CancelOrderMessage cm) {
                    recorder.onCancelSubmitted(cm.cancelOrder());
                }
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
}
