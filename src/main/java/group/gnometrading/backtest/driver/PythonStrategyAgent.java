package group.gnometrading.backtest.driver;

import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.strategies.StrategyAgent;
import java.util.List;

/**
 * Wraps a Python strategy callback as a {@link StrategyAgent} for backtest use.
 *
 * <p>Python strategies cannot directly extend a Java abstract class via JPype. Instead, they
 * implement the {@link PythonStrategyCallback} interface (which JPype can proxy), and this class
 * bridges the gap by extending {@link StrategyAgent} and delegating to the callback.
 *
 * <p>Python strategies explicitly report their simulated processing latency via
 * {@link PythonStrategyCallback#simulateProcessingTime()} because Python interpreter overhead is
 * significant compared to native Java execution.
 */
public final class PythonStrategyAgent extends StrategyAgent {

    /**
     * Callback interface implemented by the Python strategy proxy.
     *
     * <p>Returns lists of {@link Intent} objects which are then published to the intent ring
     * buffer by {@link PythonStrategyAgent}.
     */
    public interface PythonStrategyCallback {
        /** Called on each market data update. Returns intents to submit. */
        List<Intent> onMarketData(Schema data);

        /** Called on each execution report. Returns intents to submit in response. */
        List<Intent> onExecutionReport(OrderExecutionReport report);

        /** Simulated processing latency in nanoseconds (accounts for Python overhead). */
        long simulateProcessingTime();
    }

    private final PythonStrategyCallback callback;

    private PythonStrategyAgent(
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            PythonStrategyCallback callback) {
        super(marketDataBuffer, execReportBuffer, intentBuffer, positionView);
        this.callback = callback;
    }

    /**
     * Creates a {@link PythonStrategyAgent} with its own ring buffers.
     *
     * <p>Callers (e.g., gnomepy) do not need to know about ring buffer construction — the agent
     * creates them internally in the same style as {@link group.gnometrading.backtest.oms.OmsBacktestAdapter}.
     */
    public static PythonStrategyAgent create(PositionView positionView, PythonStrategyCallback callback) {
        GlobalSequence seq = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> mdBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, seq);
        SequencedRingBuffer<OrderExecutionReport> erBuffer = new SequencedRingBuffer<>(OrderExecutionReport::new, seq);
        SequencedRingBuffer<Intent> intentBuffer = new SequencedRingBuffer<>(Intent::new, seq);
        return new PythonStrategyAgent(mdBuffer, erBuffer, intentBuffer, positionView, callback);
    }

    @Override
    protected void onMarketData(Schema data) {
        List<Intent> intents = callback.onMarketData(data);
        publishIntents(intents);
    }

    @Override
    protected void onExecutionReport(OrderExecutionReport report) {
        List<Intent> intents = callback.onExecutionReport(report);
        publishIntents(intents);
    }

    @Override
    public long simulateProcessingTime() {
        return callback.simulateProcessingTime();
    }

    private void publishIntents(List<Intent> intents) {
        if (intents == null) {
            return;
        }
        for (Intent intent : intents) {
            publishIntent(intent);
        }
    }
}
