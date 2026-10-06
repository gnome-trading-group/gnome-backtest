package group.gnometrading.backtest.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import group.gnometrading.simulation.config.ExchangeProfileConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class BacktestConfig {

    /** Base seed when the config sets none, so an unconfigured run is still reproducible. */
    public static final long DEFAULT_SEED = 0x9E3779B97F4A7C15L;

    public LocalDateTime startDate;
    public LocalDateTime endDate;
    public List<ListingSimConfig> listings;
    public Map<String, ExchangeProfileConfig> profiles;
    public StrategyConfig strategy;
    /**
     * The strategy id the run trades as, which scopes its positions and risk policies as {@code strategy.id} does
     * live. 0 when the run stands for no particular strategy; {@code risk.from_registry} needs a real one.
     */
    public int strategyId = 0;

    public RiskConfig risk = new RiskConfig();
    public boolean record = true;
    public int recordDepth = 1;
    /**
     * When true, strategy processing latency is the measured wall-clock time of each {@code doWork()} call. Off by
     * default because it makes runs nondeterministic and charges Python strategies for JPype overhead.
     */
    public boolean measureProcessingTime = false;
    /**
     * Base seed for random latency models. Each listing's network and order-processing models get their own stream
     * derived from it, unless the model sets a seed itself.
     */
    public long seed = DEFAULT_SEED;

    public static BacktestConfig fromYaml(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                .registerModule(new JavaTimeModule())
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(path.toFile(), BacktestConfig.class);
    }
}
