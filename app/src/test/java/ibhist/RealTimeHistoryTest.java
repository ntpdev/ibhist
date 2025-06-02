package ibhist;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static ibhist.RealTimeHistory.ChangeState.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class RealTimeHistoryTest {

    @Test
    void notifier_price_greater_events() {
        // capture all events
        List<RealTimeHistory.PriceEvent> events = new ArrayList<>();

        // the monitor under test: threshold >120.0, minLength=3, collect to our list
        var monitor = new RealTimeHistory.PriceMonitor(new MonitorManager.MonitorData("",price -> price > 120.0, 3), events::add);

        // feed it 128 sample prices
        for (int i = 0; i < 128; i++) {
            double price = generatePrice(i, 0.0);
            monitor.test(price);
        }

        // we know there should be 2 complete crossings:
        //   first crossing yields 12 events, second yields 11, total 23
        assertThat(events).hasSize(23);

        // and we know exactly which state/price tuples they should be in order:
        assertThat(events)
                .extracting(RealTimeHistory.PriceEvent::state,
                        RealTimeHistory.PriceEvent::price)
                .containsExactly(
                        // first crossing – 1 entry + 10 insides + 1 exit
                        tuple(entry, 123.25),
                        tuple(inside, 124.00),
                        tuple(inside, 124.75),
                        tuple(inside, 125.00),
                        tuple(inside, 125.00),
                        tuple(inside, 124.75),
                        tuple(inside, 124.25),
                        tuple(inside, 123.75),
                        tuple(inside, 122.75),
                        tuple(inside, 121.50),
                        tuple(inside, 120.25),
                        tuple(exit, 118.75),
                        // second crossing – 1 entry + 9 insides + 1 exit
                        tuple(entry, 123.50),
                        tuple(inside, 124.25),
                        tuple(inside, 124.75),
                        tuple(inside, 125.00),
                        tuple(inside, 125.00),
                        tuple(inside, 124.75),
                        tuple(inside, 124.25),
                        tuple(inside, 123.50),
                        tuple(inside, 122.50),
                        tuple(inside, 121.25),
                        tuple(exit, 120.00)
                );
    }

    @Test
    void consecutive_events_required_for_events() {
        // capture events
        List<RealTimeHistory.PriceEvent> events = new ArrayList<>();

        // predicate: price < 100, minLength=2, handler collects to our list
        var monitor = new RealTimeHistory.PriceMonitor(new MonitorManager.MonitorData("", price -> price < 100.0, 2), events::add);

        // feed it a hard‐coded sequence:
        // 106 (no event),
        // 99  (first under, no event yet),
        // 105 (no event reset threshold),
        // 95  (first under, no event yet),
        // 90  (second under → ENTRY(90)),
        // 85  (still under → INSIDE(85)),
        // 110 (first outside → EXIT(110))
        double[] prices = {106, 99, 105, 95, 90, 85, 110.11};
        for (double p : prices) {
            monitor.test(p);
        }

        // we expect exactly 3 events:
        assertThat(events).hasSize(3);

        // and in order: entry@90, inside@85, exit@110
        assertThat(events)
                .extracting(RealTimeHistory.PriceEvent::state,
                        RealTimeHistory.PriceEvent::price)
                .containsExactly(
                        tuple(entry,  90.0),
                        tuple(inside, 85.0),
                        tuple(exit,   110.11)
                );
    }

    /**
     * sine wave from ~75…125 in 0.25 steps
     */
    private double generatePrice(int x, double skew) {
        return 100d + Math.round(100 * (skew * x + Math.sin(x * 0.1))) / 4d;
    }
}