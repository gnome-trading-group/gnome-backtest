package group.gnometrading.backtest.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class BacktestConfig {

    public LocalDateTime startDate;
    public LocalDateTime endDate;
    public List<ListingSimConfig> listings;
    public Map<String, ExchangeProfileConfig> profiles;
    public StrategyConfig strategy;
    public RiskConfig risk = new RiskConfig();
    public S3Config s3 = new S3Config();
    public boolean record = true;
    public int recordDepth = 1;

    public static BacktestConfig fromYaml(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                .registerModule(new JavaTimeModule())
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(path.toFile(), BacktestConfig.class);
    }
}
