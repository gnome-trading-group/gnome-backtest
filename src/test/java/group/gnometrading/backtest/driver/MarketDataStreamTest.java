package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
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
        MarketDataEntry a0 = entry(1, 0);
        MarketDataEntry a1 = entry(1, 1);
        MarketDataEntry b0 = entry(2, 0);
        MarketDataEntry b1 = entry(2, 1);
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

    private MarketDataEntry entry(int securityId, int minute) {
        return new MarketDataEntry(
                securityId, 1, SchemaType.MBP_10, START.plusMinutes(minute), MarketDataEntry.EntryType.AGGREGATED);
    }

    private void save(MarketDataEntry entry, int... secondsIn) throws IOException {
        List<Schema> records = new ArrayList<>();
        for (int second : secondsIn) {
            Mbp10Schema record = new Mbp10Schema();
            record.encoder.exchangeId(1).securityId(entry.getSecurityId()).timestampEvent(seconds(second));
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

    private static List<Long> times(List<Schema> records) {
        return records.stream().map(Schema::getEventTimestamp).toList();
    }
}
