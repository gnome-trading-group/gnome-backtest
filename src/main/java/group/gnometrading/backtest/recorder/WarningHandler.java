package group.gnometrading.backtest.recorder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/** JUL Handler that collects WARNING+ messages for retrieval from Python via JPype. */
public final class WarningHandler extends Handler {

    private final List<String> messages = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
            messages.add(record.getMessage());
        }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    public List<String> getMessages() {
        return Collections.unmodifiableList(new ArrayList<>(messages));
    }

    public void clearMessages() {
        messages.clear();
    }
}
