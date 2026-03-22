package group.gnometrading.backtest.oms;

import group.gnometrading.backtest.driver.BacktestStrategy;
import group.gnometrading.backtest.driver.LocalMessage;
import group.gnometrading.backtest.exchange.BacktestExecutionReport;
import group.gnometrading.oms.OrderManagementSystem;
import group.gnometrading.oms.order.OmsOrder;
import group.gnometrading.oms.risk.RiskCheckResult;
import group.gnometrading.schemas.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class OmsBacktestStrategy implements BacktestStrategy {

    private static final Logger logger = Logger.getLogger(OmsBacktestStrategy.class.getName());

    private final BacktestStrategy delegate;
    private final OrderManagementSystem oms;

    public OmsBacktestStrategy(BacktestStrategy delegate, OrderManagementSystem oms) {
        this.delegate = delegate;
        this.oms = oms;
    }

    @Override
    public List<LocalMessage> onMarketData(long timestamp, Schema data) {
        List<LocalMessage> messages = delegate.onMarketData(timestamp, data);
        if (messages == null || messages.isEmpty()) {
            return messages;
        }

        List<LocalMessage> filtered = new ArrayList<>();
        for (LocalMessage msg : messages) {
            if (msg instanceof LocalMessage.OrderMessage om) {
                OmsOrder omsOrder = BacktestOmsAdapter.toOmsOrder(om.order());
                RiskCheckResult result = oms.validateOrder(omsOrder);
                if (result instanceof RiskCheckResult.Approved) {
                    oms.onOrderAccepted(omsOrder);
                    filtered.add(msg);
                } else if (result instanceof RiskCheckResult.Rejected rejected) {
                    logger.warning("Order " + om.order().clientOid() + " rejected by "
                            + rejected.policyName() + ": " + rejected.reason());
                }
            } else {
                filtered.add(msg);
            }
        }
        return filtered;
    }

    @Override
    public void onExecutionReport(long timestamp, BacktestExecutionReport report) {
        oms.processExecutionReport(BacktestOmsAdapter.toOmsReport(report));
        delegate.onExecutionReport(timestamp, report);
    }

    @Override
    public long simulateProcessingTime() {
        return delegate.simulateProcessingTime();
    }

    public OrderManagementSystem getOms() {
        return oms;
    }
}
