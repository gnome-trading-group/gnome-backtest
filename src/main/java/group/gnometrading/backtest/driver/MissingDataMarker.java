package group.gnometrading.backtest.driver;

import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.StreamHeader;
import group.gnometrading.schemas.StreamRecord;
import java.util.EnumSet;
import java.util.Set;

/**
 * Where a stream's data goes missing in a run: what the gateway publishes live when a book stops being trusted, its
 * last book once more flagged bad data. The data resuming after the stretch starts with a snapshot, as live.
 */
record MissingDataMarker(int streamId, long atNanos) {

    // Records that are a whole book; a trade or a single book event re-published would read as a new one.
    static final Set<RecordType> BOOK_STATES =
            EnumSet.of(RecordType.MBP_10, RecordType.MBP_1, RecordType.BBO_1S, RecordType.BBO_1M);

    private static final int BLOCK = SbeMessage.HEADER_LENGTH;
    private static final int BYTE_MASK = 0xFF;

    /** The last book, stamped at the marker's time and flagged bad data, not a snapshot. */
    StreamRecord flag(final StreamRecord lastBook) {
        final StreamRecord marked = lastBook.recordType.newInstance();
        marked.copyFrom(lastBook);
        marked.buffer.putLong(BLOCK + StreamHeader.TIMESTAMP_EVENT_OFFSET, this.atNanos, Statics.BYTE_ORDER);
        marked.buffer.putLong(BLOCK + StreamHeader.TIMESTAMP_RECV_OFFSET, this.atNanos, Statics.BYTE_ORDER);
        final int flags = marked.buffer.getByte(BLOCK + StreamHeader.FLAGS_OFFSET) & BYTE_MASK;
        marked.buffer.putByte(BLOCK + StreamHeader.FLAGS_OFFSET, (byte)
                ((flags | 1 << StreamHeader.BAD_DATA_BIT) & ~(1 << StreamHeader.SNAPSHOT_BIT)));
        return marked;
    }
}
