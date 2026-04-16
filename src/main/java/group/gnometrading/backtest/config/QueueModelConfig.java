package group.gnometrading.backtest.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import group.gnometrading.backtest.queues.OptimisticQueueModel;
import group.gnometrading.backtest.queues.ProbabilisticQueueModel;
import group.gnometrading.backtest.queues.QueueModel;
import group.gnometrading.backtest.queues.RiskAverseQueueModel;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = QueueModelConfig.RiskAverse.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = QueueModelConfig.Optimistic.class, name = "optimistic"),
    @JsonSubTypes.Type(value = QueueModelConfig.RiskAverse.class, name = "risk_averse"),
    @JsonSubTypes.Type(value = QueueModelConfig.Probabilistic.class, name = "probabilistic")
})
public abstract class QueueModelConfig {

    public abstract QueueModel toModel();

    public static final class Optimistic extends QueueModelConfig {
        @Override
        public QueueModel toModel() {
            return new OptimisticQueueModel();
        }
    }

    public static final class RiskAverse extends QueueModelConfig {
        @Override
        public QueueModel toModel() {
            return new RiskAverseQueueModel();
        }
    }

    public static final class Probabilistic extends QueueModelConfig {
        public double cancelAheadProbability = 0.5;

        @Override
        public QueueModel toModel() {
            return new ProbabilisticQueueModel(cancelAheadProbability);
        }
    }
}
