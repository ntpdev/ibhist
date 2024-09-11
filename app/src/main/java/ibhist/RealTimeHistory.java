package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class RealTimeHistory {
    private static final Logger log = LogManager.getLogger(RealTimeHistory.class);

    public static final int MAX_SIZE = 1024;
    // RealTimeBar(LocalDateTime dt, double open, double high, double low, double close, double volume, double wap) {}
    LocalDateTime[] dt = new LocalDateTime[MAX_SIZE];
    double[] open = new double[MAX_SIZE];
    double[] high = new double[MAX_SIZE];
    double[] low = new double[MAX_SIZE];
    double[] close = new double[MAX_SIZE];
    double[] volume = new double[MAX_SIZE];

    int n = 0;

    int add(RealTimeBar bar) {
        dt[n] = bar.dt();
        open[n] = bar.open();
        high[n] = bar.high();
        low[n] = bar.low();
        close[n] = bar.close();
        volume[n] = bar.volume();
        ++n;
        return n;
    }

    int size() {
        return n;
    }

    static private int minutesSinceMidnight(LocalDateTime dt) {
        return 60 * dt.getHour() + dt.getMinute();
    }

    List<PriceHistory.Bar> toPriceBars() {
        var xs = new ArrayList<PriceHistory.Bar>();
        int first = 0;
        for (int i = 0; i < n; ++i) {
            if (minutesSinceMidnight(dt[first]) != minutesSinceMidnight(dt[i]) || i == n-1) {
                xs.add(aggregate(first, i));
                first = i;
            }
        }
        return xs;
    }

    /**
     * aggregate real-time bars into a single bar from start to end (exclusive)
     */
    PriceHistory.Bar aggregate(int start, int end) {
        if (end >= n) {
            throw new ArrayIndexOutOfBoundsException(end);
        }
        double maxH = high[start];
        double minL = low[start];
        double v = 0;
        for (int i = start; i < end; ++i) {
            maxH = Math.max(maxH, high[i]);
            minL = Math.min(minL, low[i]);
            v += volume[i];
        }
        return new PriceHistory.Bar(dt[start], dt[end-1], open[start], maxH, minL, close[end-1], v, 0);
    }

    public static PriceCondition newCountNotifier(PriceCondition condition, int threshhold, Consumer<PriceEvent> listener) {
        return new CountNotifier(condition, threshhold, listener);
    }

    @FunctionalInterface
    public interface PriceCondition {
        boolean test(double f);
    }

    enum ChangeState {
        entry,
        exit,
        inside
    }

    record PriceEvent(ChangeState state, double price) {}

    /**
     * ChangeNotifier fires events while pred true provided at least threshold number of events
     */
    static class CountNotifier implements PriceCondition{
        protected final int threshold;
        protected final PriceCondition condition;
        protected final Consumer<PriceEvent> listener;
        private int count = 0;
        private boolean isInside = false;

        CountNotifier(PriceCondition condition, int threshhold, Consumer<PriceEvent> listener) {
            this.threshold = threshhold;
            this.condition = condition;
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public boolean test(double f) {
            boolean prior = isInside;
            count = condition.test(f) ? count + 1 : 0;
            isInside = count >= threshold;
            if (isInside) {
                if (prior) {
                    fireEvent(ChangeState.inside, f);
                } else {
                    fireEvent(ChangeState.entry, f);
                }
            } else if (prior) {
                fireEvent(ChangeState.exit, f);
            }
            return isInside;
        }

        protected void fireEvent(ChangeState state, double f) {
            listener.accept(new PriceEvent(state, f));
        }
    }
}
