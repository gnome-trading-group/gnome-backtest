package group.gnometrading.backtest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** What a run does where collection says its market data is missing. */
public enum OnMissing {
    /** Mark each stream bad where its data goes missing, as the gateway does live. */
    @JsonProperty("mark")
    MARK,
    /** Refuse to run over missing data. */
    @JsonProperty("fail")
    FAIL,
    /** Replay as recorded; still reported. */
    @JsonProperty("ignore")
    IGNORE
}
