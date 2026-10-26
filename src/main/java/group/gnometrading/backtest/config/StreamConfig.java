package group.gnometrading.backtest.config;

/** A stream the strategy reads besides its listings' books (a vendor feed, say), and which of its records to replay. */
public class StreamConfig {

    public int streamId;
    /** The record type's identifier in stored data, e.g. {@code mbp-10}. */
    public String recordType;
}
