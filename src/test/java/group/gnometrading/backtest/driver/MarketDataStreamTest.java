package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MarketDataStreamTest {

    private static final String BUCKET = "test";
    private static final LocalDateTime START = LocalDateTime.of(2026, 8, 24, 0, 0);
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final InMemoryS3 s3 = new InMemoryS3();

    @Test
    void handsBackOneMinuteAtATimeInEntryOrder() throws IOException {
        // Listing-major, as the factory builds them; listing 2 had no activity in the second minute.
        StreamEntry a0 = entry(1, 0);
        StreamEntry a1 = entry(1, 1);
        StreamEntry b0 = entry(2, 0);
        StreamEntry b1 = entry(2, 1);
        save(a0, 10, 11);
        save(a1, 70);
        save(b0, 5);

        for (int ahead : new int[] {1, 2, 10}) {
            MarketDataStream stream = new MarketDataStream(List.of(a0, a1, b0, b1), s3, BUCKET, 4, ahead);

            assertTrue(stream.hasNext());
            assertEquals(nanos(START), stream.nextMinuteStartNanos());
            assertEquals(List.of(seconds(10), seconds(11), seconds(5)), times(stream.next()), "ahead=" + ahead);
            assertEquals(nanos(START.plusMinutes(1)), stream.nextMinuteStartNanos());
            assertEquals(List.of(seconds(70)), times(stream.next()), "ahead=" + ahead);
            assertFalse(stream.hasNext());
            assertEquals(4, stream.files());
            assertEquals(1, stream.missing());
        }
    }

    @Test
    void minutesWithoutAFileAreNeverAskedFor() throws IOException {
        List<StreamEntry> entries = new ArrayList<>();
        for (int minute = 0; minute < 60; minute++) {
            entries.add(entry(1, minute));
        }
        save(entry(1, 7), 7 * 60);
        save(entry(1, 42), 42 * 60);

        MarketDataStream stream = new MarketDataStream(entries, s3, BUCKET, 4, 10);

        assertEquals(nanos(START.plusMinutes(7)), stream.nextMinuteStartNanos());
        assertEquals(List.of(seconds(7 * 60)), times(stream.next()));
        assertEquals(List.of(seconds(42 * 60)), times(stream.next()));
        assertFalse(stream.hasNext());
        assertEquals(2, s3.gets());
        assertEquals(60, stream.files());
        assertEquals(58, stream.missing());
    }

    @Test
    void aRunAcrossMidnightKeepsOnlyItsOwnMinutesOfEachDay() throws IOException {
        LocalDateTime lateFirstDay = START.plusHours(23).plusMinutes(58);
        List<StreamEntry> entries = new ArrayList<>();
        for (int minute = 0; minute < 4; minute++) {
            entries.add(new StreamEntry(1, RecordType.MBP_10, lateFirstDay.plusMinutes(minute)));
        }
        // Files before and after the run on both of its days, and one in the run on each day.
        save(new StreamEntry(1, RecordType.MBP_10, START), 0);
        save(entries.get(1), 0);
        save(entries.get(2), 0);
        save(new StreamEntry(1, RecordType.MBP_10, lateFirstDay.plusMinutes(10)), 0);

        MarketDataStream stream = new MarketDataStream(entries, s3, BUCKET, 2, 10);

        assertEquals(nanos(lateFirstDay.plusMinutes(1)), stream.nextMinuteStartNanos());
        stream.next();
        assertEquals(nanos(lateFirstDay.plusMinutes(2)), stream.nextMinuteStartNanos());
        stream.next();
        assertFalse(stream.hasNext());
        assertEquals(2, s3.gets());
    }

    @Test
    void aRunWithNoFilesHasNothingToHandBack() {
        MarketDataStream stream = new MarketDataStream(List.of(entry(1, 0), entry(2, 0)), s3, BUCKET, 2, 10);

        assertFalse(stream.hasNext());
        assertEquals(2, stream.files());
        assertEquals(2, stream.missing());
        assertEquals(0, s3.gets());
    }

    private StreamEntry entry(int securityId, int minute) {
        return new StreamEntry(securityId, RecordType.MBP_10, START.plusMinutes(minute));
    }

    private void save(StreamEntry entry, int... secondsIn) throws IOException {
        List<StreamRecord> records = new ArrayList<>();
        for (int second : secondsIn) {
            Mbp10Schema record = new Mbp10Schema();
            record.encoder.streamId(entry.getStreamId()).timestampEvent(seconds(second));
            records.add(record);
        }
        entry.saveToS3(s3, BUCKET, records);
    }

    private static long seconds(int second) {
        return nanos(START) + second * NANOS_PER_SECOND;
    }

    private static long nanos(LocalDateTime time) {
        return time.toEpochSecond(ZoneOffset.UTC) * NANOS_PER_SECOND;
    }

    private static List<Long> times(List<StreamRecord> records) {
        return records.stream().map(StreamRecord::getEventTimestamp).toList();
    }
}
