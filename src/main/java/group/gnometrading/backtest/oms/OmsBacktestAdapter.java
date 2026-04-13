package group.gnometrading.backtest.oms;

import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.oms.OmsAgent;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.state.TrackedOrder;
import group.gnometrading.schemas.CancelOrderDecoder;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.ModifyOrderDecoder;
import group.gnometrading.schemas.OrderDecoder;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Side;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Bridges the OMS agent loop to BacktestDriver's LocalMessage format.
 *
 * <p>Writes intents and execution reports to the OMS agent's inbound ring buffers,
 * steps {@link OmsAgent#doWork()} to process them, then drains the outbound ring buffer
 * to produce {@link LocalMessage} objects for the simulated exchange.
 */
public final class OmsBacktestAdapter {

    private final OmsAgent omsAgent;
    private final OrderManagementSystem oms;
    private final BacktestRecorder recorder;
    private final List<LocalMessage> messageBuffer = new ArrayList<>();

    // Ring buffers shared between this adapter and the OmsAgent
    private final SequencedRingBuffer<Intent> intentBuffer;
    private final SequencedRingBuffer<OrderExecutionReport> execReportBuffer;
    private final SequencedRingBuffer<Intent> outboundBuffer;

    // Poller for draining the outbound ring buffer
    private final group.gnometrading.sequencer.SequencedPoller outboundPoller;

    // Pre-allocated decoders for reading outbound events
    private final group.gnometrading.schemas.Order orderDecodeMsg = new group.gnometrading.schemas.Order();
    private final group.gnometrading.schemas.CancelOrder cancelDecodeMsg = new group.gnometrading.schemas.CancelOrder();
    private final group.gnometrading.schemas.ModifyOrder modifyDecodeMsg = new group.gnometrading.schemas.ModifyOrder();

    private static final int OUTBOUND_BUFFER_SIZE = 64;

    public OmsBacktestAdapter(OrderManagementSystem oms) {
        this(oms, null);
    }

    public OmsBacktestAdapter(OrderManagementSystem oms, BacktestRecorder recorder) {
        this.oms = oms;
        this.recorder = recorder;

        GlobalSequence globalSequence = new GlobalSequence();
        this.intentBuffer = new SequencedRingBuffer<>(Intent::new, globalSequence);
        this.execReportBuffer = new SequencedRingBuffer<>(OrderExecutionReport::new, globalSequence);
        this.outboundBuffer = new SequencedRingBuffer<>(Intent::new, globalSequence, OUTBOUND_BUFFER_SIZE);

        this.omsAgent = new OmsAgent(oms, intentBuffer, execReportBuffer, outboundBuffer);
        this.outboundPoller = outboundBuffer.createPoller(this::onOutboundEvent);

        // Backtest runs single-threaded — don't start Disruptor consumer threads.
        // doWork() uses pollers to drive the event loop explicitly.
    }

    public List<LocalMessage> processIntents(long timestamp, Intent[] intents, int count) throws Exception {
        messageBuffer.clear();
        for (int i = 0; i < count; i++) {
            if (recorder != null) {
                recorder.onIntent(timestamp, intents[i]);
            }
            Intent slot = intentBuffer.claim();
            slot.buffer.putBytes(0, intents[i].buffer, 0, intents[i].totalMessageSize());
            slot.wrap(slot.buffer);
            intentBuffer.publish();
        }
        omsAgent.doWork();
        outboundPoller.poll();
        return messageBuffer;
    }

    public List<LocalMessage> processIntents(long timestamp, List<Intent> intents) throws Exception {
        messageBuffer.clear();
        for (Intent intent : intents) {
            if (recorder != null) {
                recorder.onIntent(timestamp, intent);
            }
            Intent slot = intentBuffer.claim();
            slot.buffer.putBytes(0, intent.buffer, 0, intent.totalMessageSize());
            slot.wrap(slot.buffer);
            intentBuffer.publish();
        }
        omsAgent.doWork();
        outboundPoller.poll();
        return messageBuffer;
    }

    public List<LocalMessage> processExecutionReport(OrderExecutionReport report) throws Exception {
        if (recorder != null) {
            long clientOid = report.getClientOidCounter();
            long orderPrice = 0;
            long orderSize = 0;
            Side side = Side.None;
            TrackedOrder tracked = oms.getOrder(clientOid);
            if (tracked != null) {
                orderPrice = tracked.getPrice();
                orderSize = tracked.getSize();
                side = tracked.getSide();
            }
            recorder.onExecutionReport(
                    report.decoder.timestampRecv(),
                    report,
                    tracked != null ? tracked.getStrategyId() : 0,
                    side,
                    orderPrice,
                    orderSize);
        }
        messageBuffer.clear();
        OrderExecutionReport slot = execReportBuffer.claim();
        slot.buffer.putBytes(0, report.buffer, 0, report.totalMessageSize());
        slot.wrap(slot.buffer);
        execReportBuffer.publish();
        omsAgent.doWork();
        outboundPoller.poll();
        return messageBuffer;
    }

    private void onOutboundEvent(long globalSeq, int templateId, org.agrona.concurrent.UnsafeBuffer buf, int len)
            throws Exception {
        if (templateId == OrderDecoder.TEMPLATE_ID) {
            group.gnometrading.schemas.Order order = new group.gnometrading.schemas.Order();
            order.buffer.putBytes(0, buf, 0, len);
            order.wrap(order.buffer);
            messageBuffer.add(new LocalMessage.OrderMessage(order));
        } else if (templateId == CancelOrderDecoder.TEMPLATE_ID) {
            group.gnometrading.schemas.CancelOrder cancel = new group.gnometrading.schemas.CancelOrder();
            cancel.buffer.putBytes(0, buf, 0, len);
            cancel.wrap(cancel.buffer);
            messageBuffer.add(new LocalMessage.CancelOrderMessage(cancel));
        } else if (templateId == ModifyOrderDecoder.TEMPLATE_ID) {
            group.gnometrading.schemas.ModifyOrder modify = new group.gnometrading.schemas.ModifyOrder();
            modify.buffer.putBytes(0, buf, 0, len);
            modify.wrap(modify.buffer);
            messageBuffer.add(new LocalMessage.ModifyOrderMessage(modify));
        }
    }

    public OrderManagementSystem getOms() {
        return oms;
    }
}
