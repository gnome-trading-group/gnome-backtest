package group.gnometrading.backtest.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import group.gnometrading.backtest.driver.BacktestDriver;
import group.gnometrading.data.MissingData;
import group.gnometrading.data.MissingStretch;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApplyMissingTest {

    private static final List<MissingStretch> STRETCHES = List.of(new MissingStretch(10, 20, "TASK_LOST"));
    private static final List<MissingData> MISSING =
            List.of(new MissingData(7, 70, STRETCHES, 10, 0), new MissingData(9, 0, List.of(), 0, 5));

    private final BacktestDriver driver = mock(BacktestDriver.class);

    private BacktestConfig config(OnMissing onMissing) {
        BacktestConfig config = new BacktestConfig();
        config.onMissing = onMissing;
        return config;
    }

    @Test
    void markFlagsEachStreamWhereItsDataIsMissingAndReportsIt() {
        BacktestDriverFactory.applyMissing(config(OnMissing.MARK), driver, MISSING);

        verify(driver).reportMissingData(MISSING);
        verify(driver).markMissing(7, STRETCHES);
        verify(driver).markMissing(9, List.of());
    }

    @Test
    void ignoreOnlyReports() {
        BacktestDriverFactory.applyMissing(config(OnMissing.IGNORE), driver, MISSING);

        verify(driver).reportMissingData(MISSING);
        verify(driver, never()).markMissing(anyInt(), anyList());
    }

    @Test
    void failRefusesARunWithMissingDataButNotOneWithOnlyUntrackedTime() {
        assertThrows(
                IllegalStateException.class,
                () -> BacktestDriverFactory.applyMissing(config(OnMissing.FAIL), driver, MISSING));

        BacktestDriverFactory.applyMissing(
                config(OnMissing.FAIL), driver, List.of(new MissingData(9, 0, List.of(), 0, 5)));
    }
}
