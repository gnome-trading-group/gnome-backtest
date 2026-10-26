package group.gnometrading.backtest.driver;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.StreamRecord;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Market data one minute at a time, downloaded a fixed number of minutes ahead on background threads.
 *
 * <p>Each stored file holds one listing's records for one minute of event time, so a backtest only needs the minutes
 * around the time it has reached. Background threads only download and decode, each into its own list; the caller's
 * thread takes the finished lists in a fixed order, so which download finishes first never changes what it sees.
 */
final class MarketDataStream {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final S3Client s3Client;
    private final String bucket;
    private final ExecutorService pool;
    private final List<Map.Entry<LocalDateTime, List<StreamEntry>>> minutes;
    private final ArrayDeque<List<Future<List<StreamRecord>>>> inFlight = new ArrayDeque<>();
    private int nextToSubmit = 0;
    private int nextToTake = 0;
    private int files = 0;
    private int missing = 0;

    /**
     * Groups {@code entries} by minute, keeping their order within each minute, and starts downloading the first
     * {@code minutesAhead} minutes.
     */
    MarketDataStream(List<StreamEntry> entries, S3Client s3Client, String bucket, int threads, int minutesAhead) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        TreeMap<LocalDateTime, List<StreamEntry>> byMinute = new TreeMap<>();
        for (StreamEntry entry : entries) {
            byMinute.computeIfAbsent(entry.getTimestamp(), k -> new ArrayList<>())
                    .add(entry);
        }
        this.minutes = new ArrayList<>(byMinute.entrySet());
        this.pool = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, "market-data-loader");
            // A run stopped part way must not keep the JVM alive for downloads nobody will read.
            thread.setDaemon(true);
            return thread;
        });
        for (int i = 0; i < minutesAhead; i++) {
            submitNext();
        }
    }

    boolean hasNext() {
        return nextToTake < minutes.size();
    }

    /** Where the next minute starts, in epoch nanoseconds. */
    long nextMinuteStartNanos() {
        return minutes.get(nextToTake).getKey().toEpochSecond(ZoneOffset.UTC) * NANOS_PER_SECOND;
    }

    /**
     * The next minute's records: each file's in stored order, files in the order the entries were given. Waits for
     * downloads still running, then starts the minute that keeps the same number ahead.
     */
    List<StreamRecord> next() {
        List<Future<List<StreamRecord>>> minute = inFlight.poll();
        nextToTake++;
        submitNext();
        List<StreamRecord> records = new ArrayList<>();
        try {
            for (Future<List<StreamRecord>> file : minute) {
                records.addAll(file.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Market data loading interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Market data loading failed", e.getCause());
        }
        if (!hasNext()) {
            pool.shutdown();
        }
        return records;
    }

    /** Files read so far, missing ones included. */
    synchronized int files() {
        return files;
    }

    /** Files with no object in S3, which is normal for a minute with no activity. */
    synchronized int missing() {
        return missing;
    }

    private void submitNext() {
        if (nextToSubmit >= minutes.size()) {
            return;
        }
        List<StreamEntry> entries = minutes.get(nextToSubmit++).getValue();
        List<Future<List<StreamRecord>>> futures = new ArrayList<>(entries.size());
        for (StreamEntry entry : entries) {
            futures.add(pool.submit(() -> load(entry)));
        }
        inFlight.add(futures);
    }

    private List<StreamRecord> load(StreamEntry entry) {
        try {
            List<StreamRecord> records = entry.loadFromS3(s3Client, bucket);
            count(false);
            return records;
        } catch (NoSuchKeyException e) {
            count(true);
            return Collections.emptyList();
        }
    }

    private synchronized void count(boolean wasMissing) {
        files++;
        if (wasMissing) {
            missing++;
        }
    }
}
