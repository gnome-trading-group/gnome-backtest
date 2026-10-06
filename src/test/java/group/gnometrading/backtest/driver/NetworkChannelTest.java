package group.gnometrading.backtest.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class NetworkChannelTest {

    @Test
    void aSlowMessageHoldsUpTheOnesBehindIt() {
        NetworkChannel channel = new NetworkChannel();

        assertEquals(1_000, channel.arrival(0, 1_000));
        assertEquals(1_000, channel.arrival(10, 5), "sent later but drew less latency: waits for the first");
        assertEquals(1_020, channel.arrival(20, 1_000), "drew more: arrives when its own latency says");
    }

    @Test
    void messagesThatDoNotCrossArriveWhenTheirLatencySays() {
        NetworkChannel channel = new NetworkChannel();

        assertEquals(100, channel.arrival(0, 100));
        assertEquals(300, channel.arrival(200, 100));
    }
}
