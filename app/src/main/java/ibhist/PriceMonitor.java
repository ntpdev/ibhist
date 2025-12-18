package ibhist;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.DoublePredicate;

import static ibhist.ChangeState.*;

/**
 * PriceMonitor fires events while pred true provided at least threshold number of events
 */
public class PriceMonitor implements DoublePredicate {
    protected final MonitorManager.MonitorData data;
    protected final Consumer<PriceEvent> listener;
    private int count = 0;
    private boolean isInside = false;

    PriceMonitor(MonitorManager.MonitorData data, Consumer<PriceEvent> listener) {
        this.data = data;
        this.listener = Objects.requireNonNull(listener);
    }

    @Override
    public boolean test(double f) {
        boolean prior = isInside;
        count = data.test(f) ? count + 1 : 0;
        isInside = data.meetsThreshold(count);
        if (isInside) {
            if (prior) {
                fireEvent(inside, f);
            } else {
                fireEvent(entry, f);
            }
        } else {
            if (prior)
                fireEvent(exit, f);
        }
        return isInside;
    }

    protected void fireEvent(ChangeState state, double f) {
        listener.accept(new PriceEvent(state, f));
    }
}
