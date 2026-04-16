package group.gnometrading.backtest.oms;

import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.recorder.BacktestRecorder;
import group.gnometrading.oms.OmsAgent;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.state.TrackedOrder;
import group.gnometrading.schemas.CancelOrder;
import group.gnometrading.schemas.CancelOrderDecoder;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.ModifyOrder;
import group.gnometrading.schemas.ModifyOrderDecoder;
import group.gnometrading.schemas.Order;
import group.gnometrading.schemas.OrderDecoder;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderExecutionReportDecoder;
import group.gnometrading.schemas.Side;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Bridges the OMS agent loop to BacktestDriver's LocalMessage format.
 *
 * <p>Writes intents and execution reports to the OMS agent's inbound ring buffers,
 * steps {@link OmsAgent#doWork()} to process them, then drains the outbound ring buffer
 * to produce {@link LocalMessage} objects for the simulated exchange.
 *
 * <p>After each {@code doWork()} call, the strategy exec report buffer is also drained.
 * These exec reports (both forwarded gateway reports and synthetic risk rejections) are
 * exposed via {@link #getStrategyExecReports()} for the driver to deliver to the strategy.
 */
public final class OmsBacktestAdapter {

    private final OmsAgent omsAgent;
    private final OrderManagementSystem oms;
    private final BacktestRecorder recorder;
    private final List<LocalMessage> messageBuffer = new ArrayList<>();
    private final List<OrderExecutionReport> strategyExecReports = new ArrayList<>();

    // Ring buffers shared between this adapter and the OmsAgent
    private final SequencedRingBuffer<Intent> intentBuffer;
    private final SequencedRingBuffer<OrderExecutionReport> execReportBuffer;
    private final SequencedRingBuffer<Intent> orderOutboundBuffer;
    private final SequencedRingBuffer<OrderExecutionReport> strategyExecReportBuffer;

    // Pollers for draining the outbound ring buffers
    private final SequencedPoller orderOutboundPoller;
    private final SequencedPoller strategyExecReportPoller;

    private static final int OUTBOUND_BUFFER_SIZE = 64;

    public OmsBacktestAdapter(final OrderManagementSystem oms) {
        this(oms, null);
    }

    public OmsBacktestAdapter(final OrderManagementSystem oms, final BacktestRecorder recorder) {
        this.oms = oms;
        this.recorder = recorder;

        GlobalSequence globalSequence = new GlobalSequence();
        this.intentBuffer = new SequencedRingBuffer<>(Intent::new, globalSequence);
        this.execReportBuffer = new SequencedRingBuffer<>(OrderExecutionReport::new, globalSequence);
        this.orderOutboundBuffer = new SequencedRingBuffer<>(Intent::new, globalSequence, OUTBOUND_BUFFER_SIZE);
        this.strategyExecReportBuffer =
                new SequencedRingBuffer<>(OrderExecutionReport::new, globalSequence, OUTBOUND_BUFFER_SIZE);

        this.omsAgent =
                new OmsAgent(oms, intentBuffer, execReportBuffer, orderOutboundBuffer, strategyExecReportBuffer);
        this.orderOutboundPoller = orderOutboundBuffer.createPoller(this::onOrderOutboundEvent);
        this.strategyExecReportPoller = strategyExecReportBuffer.createPoller(this::onStrategyExecReportEvent);

        // Backtest runs single-threaded — don't start Disruptor consumer threads.
        // doWork() uses pollers to drive the event loop explicitly.
    }

    public List<LocalMessage> processIntents(final long timestamp, final Intent[] intents, final int count)
            throws Exception {
        messageBuffer.clear();
        strategyExecReports.clear();
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
        orderOutboundPoller.poll();
        strategyExecReportPoller.poll();
        return messageBuffer;
    }

    public List<LocalMessage> processIntents(final long timestamp, final List<Intent> intents) throws Exception {
        messageBuffer.clear();
        strategyExecReports.clear();
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
        orderOutboundPoller.poll();
        strategyExecReportPoller.poll();
        return messageBuffer;
    }

    public List<LocalMessage> processExecutionReport(final OrderExecutionReport report) throws Exception {
        if (recorder != null) {
            long clientOid = report.getClientOidCounter();
            Side side = Side.None;
            int strategyId = 0;
            TrackedOrder tracked = oms.getOrder(clientOid);
            if (tracked != null) {
                side = tracked.getSide();
                strategyId = tracked.getStrategyId();
            }
            recorder.onExecution(report.decoder.timestampRecv(), report, strategyId, side);
        }
        messageBuffer.clear();
        strategyExecReports.clear();
        OrderExecutionReport slot = execReportBuffer.claim();
        slot.buffer.putBytes(0, report.buffer, 0, report.totalMessageSize());
        slot.wrap(slot.buffer);
        execReportBuffer.publish();
        omsAgent.doWork();
        orderOutboundPoller.poll();
        strategyExecReportPoller.poll();
        return messageBuffer;
    }

    /** Returns exec reports forwarded from the OMS to the strategy (real and synthetic rejections). */
    public List<OrderExecutionReport> getStrategyExecReports() {
        return strategyExecReports;
    }

    private void onOrderOutboundEvent(final long globalSeq, final int templateId, final UnsafeBuffer buf, final int len)
            throws Exception {
        if (templateId == OrderDecoder.TEMPLATE_ID) {
            Order order = new Order();
            order.buffer.putBytes(0, buf, 0, len);
            order.wrap(order.buffer);
            messageBuffer.add(new LocalMessage.OrderMessage(order));
        } else if (templateId == CancelOrderDecoder.TEMPLATE_ID) {
            CancelOrder cancel = new CancelOrder();
            cancel.buffer.putBytes(0, buf, 0, len);
            cancel.wrap(cancel.buffer);
            messageBuffer.add(new LocalMessage.CancelOrderMessage(cancel));
        } else if (templateId == ModifyOrderDecoder.TEMPLATE_ID) {
            ModifyOrder modify = new ModifyOrder();
            modify.buffer.putBytes(0, buf, 0, len);
            modify.wrap(modify.buffer);
            messageBuffer.add(new LocalMessage.ModifyOrderMessage(modify));
        }
    }

    private void onStrategyExecReportEvent(
            final long globalSeq, final int templateId, final UnsafeBuffer buf, final int len) throws Exception {
        if (templateId == OrderExecutionReportDecoder.TEMPLATE_ID) {
            OrderExecutionReport copy = new OrderExecutionReport();
            copy.buffer.putBytes(0, buf, 0, len);
            copy.wrap(copy.buffer);
            strategyExecReports.add(copy);
        }
    }

    public OrderManagementSystem getOms() {
        return oms;
    }
}
