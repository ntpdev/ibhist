package ibhist;

import java.time.LocalDateTime;

public class RealTimeHistory {
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

    PriceHistory.Bar aggregrateLast(int sz) {
        int first = sz >= n ? 0 : n - sz;
        int last = n - 1;
        double maxH = high[first];
        double minL = low[first];
        double v = 0;
        for (int i = first; i < n; i++) {
            maxH = Math.max(maxH, high[i]);
            minL = Math.min(minL, low[i]);
            v += volume[i];
        }
        return new PriceHistory.Bar(dt[first], dt[last], open[first], maxH, minL, close[last], v, 0);
    }
}
