package ibhist;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

class RealTimeHistoryTest {

    private static final Logger log = LogManager.getLogger(RealTimeHistoryTest.class.getSimpleName());

    @Test
    void test_notifier() {
        var x = new RealTimeHistory.CountNotifier(e -> e > 120.0, 3, this::handler);

        int n = 128;
        var values = new double[n];
        var output = new boolean[n];
        for (int i = 0; i < n; ++i) {
            values[i] = generatePrice(i, 0);
        }
        for (int i = 0; i < n; ++i) {
            output[i] = x.test(values[i]);
            log.info(String.format("%2d %.2f %s", i, values[i], output[i]));
        }

    }

    void handler(RealTimeHistory.PriceEvent event) {
        log.info(event);
    }

    // sin curve between 75 and 125 with 0.25 steps
    double generatePrice(int x, double skew) {
        return 100d + Math.round(100 * (skew * x + Math.sin(x * 0.1))) / 4d;
    }
}