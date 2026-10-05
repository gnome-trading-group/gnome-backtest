package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.junit.jupiter.api.Test;

class BacktestEventTest {

    @Test
    void sameTimestampAndTypeComeOutInInsertionOrder() {
        PriorityQueue<BacktestEvent> queue = new PriorityQueue<>();
        for (int i = 0; i < 1_000; i++) {
            queue.add(new BacktestEvent(100L, EventType.EXCHANGE_MESSAGE, i, i));
        }

        List<Object> polled = new ArrayList<>();
        while (!queue.isEmpty()) {
            polled.add(queue.poll().data());
        }

        for (int i = 0; i < 1_000; i++) {
            assertEquals(i, polled.get(i));
        }
    }

    @Test
    void timestampThenTypeOutrankSequence() {
        PriorityQueue<BacktestEvent> queue = new PriorityQueue<>();
        queue.add(new BacktestEvent(200L, EventType.EXCHANGE_MARKET_DATA, 0, "later"));
        queue.add(new BacktestEvent(100L, EventType.LOCAL_MESSAGE, 1, "local message"));
        queue.add(new BacktestEvent(100L, EventType.LOCAL_MARKET_DATA, 2, "local data"));
        queue.add(new BacktestEvent(100L, EventType.EXCHANGE_MESSAGE, 3, "exchange message"));
        queue.add(new BacktestEvent(100L, EventType.EXCHANGE_MARKET_DATA, 4, "exchange data"));

        List<Object> polled = new ArrayList<>();
        while (!queue.isEmpty()) {
            polled.add(queue.poll().data());
        }

        assertEquals(List.of("exchange data", "exchange message", "local data", "local message", "later"), polled);
    }
}
