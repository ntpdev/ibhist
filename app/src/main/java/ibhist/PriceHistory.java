package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class PriceHistory implements Serializable {
    private static final Logger log = LogManager.getLogger("PriceHistory");

    private final String symbol;
    private int size = 0;
    private int max_size = 1024;
    LocalDateTime[] dates;
    List<Column> columns = new ArrayList<>();
    private transient Index index = null;

    PriceHistory(String symbol, int size, String... names) {
        this.symbol = symbol;
        this.max_size = size;
        for (String name : names) {
            if (name.equals("date")) {
                dates = new LocalDateTime[max_size];
            } else {
                columns.add(new Column(name, max_size));
            }
        }
    }

    int length() {
        return size;
    }

    // update actual size used when adding filled columns
    void setLength(int value) {
        size = value;
    }

    void expand() {
        int sz = max_size * 2;
        if (dates != null) {
            var xs = new LocalDateTime[sz];
            System.arraycopy(dates, 0, xs, 0, max_size);
            dates = xs;
        }
        columns.replaceAll(column -> column.expand(sz));
        max_size = sz;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getSymbolLowerCase() {
        return symbol.toLowerCase();
    }

    public LocalDateTime[] getDates() {
        return dates;
    }

    public double[] getColumn(String name) {
        var xs = findColumn(name);
        if (xs == null) {
            throw new RuntimeException("column not found " + name);
        }
        return xs;
    }

    public double[] findColumn(String name) {
        for (var c : columns) {
            if (c.name.equals(name)) {
                return c.values;
            }
        }
        return null;
    }

    public double[] setColumnValues(String name, double[] xs) {
        if (xs.length != max_size) {
            throw new IllegalArgumentException("lengths do not match " + xs.length);
        }
        var c = getColumn(name);
        System.arraycopy(xs, 0, c, 0, xs.length);
        return c;
    }

    public int find(LocalDateTime target) {
        return Arrays.binarySearch(dates, target);
    }

    public int floor(LocalDateTime target) {
        int p = find(target);
        if (p >= 0) {
            return p;
        }
        p = -(p + 2);
        if (p == 0) {
            throw new RuntimeException("date " + target + " is before start of the PriceHistory ");
        }
        return p;
    }

    public PriceHistory insert(int index, LocalDateTime date, double open, double high, double low, double close, double volume) {
        expandIfNecessary(index);
        dates[index] = date;
        columns.get(0).values[index] = open;
        columns.get(1).values[index] = high;
        columns.get(2).values[index] = low;
        columns.get(3).values[index] = close;
        columns.get(4).values[index] = volume;
        ++size;
        return this;
    }

    public PriceHistory insert(int index, LocalDateTime date, double open, double high, double low, double close, double volume, double vwap) {
        expandIfNecessary(index);
        dates[index] = date;
        columns.get(0).values[index] = open;
        columns.get(1).values[index] = high;
        columns.get(2).values[index] = low;
        columns.get(3).values[index] = close;
        columns.get(4).values[index] = volume;
        columns.get(5).values[index] = vwap;
        ++size;
        return this;
    }

    public PriceHistory add(LocalDateTime date, double open, double high, double low, double close, double volume, double vwap) {
        return insert(size, date, open, high, low, close, volume, vwap);
    }

    public PriceHistory add(LocalDateTime date, double open, double high, double low, double close, double volume) {
        return insert(size, date, open, high, low, close, volume);
    }

    public PriceHistory replace(int index, LocalDateTime date, double open, double high, double low, double close, double volume) {
        int i = index >= 0 ? index : length() + index;
        dates[i] = date;
        columns.get(0).values[i] = open;
        columns.get(1).values[i] = high;
        columns.get(2).values[i] = low;
        columns.get(3).values[i] = close;
        columns.get(4).values[i] = volume;
        return this;
    }

    private void expandIfNecessary(int index) {
        if (index >= max_size) {
            expand();
        }
    }

    Column average(String a, String b, String output) {
        var as = getColumn(a);
        var bs = getColumn(b);
        var c = newColumn(output);
        for (int i = 0; i < length(); i++) {
            c.values[i] = (as[i] + bs[i]) * .5;
        }
        return c;
    }

    Column cumulative(String a, String output) {
        var as = getColumn(a);
        var c = newColumn(output);
        double cumulative = 0;
        for (int i = 0; i < length(); i++) {
            cumulative += as[i];
            c.values[i] = cumulative;
        }
        return c;
    }

    Column ema(String a, String output, int period) {
        var as = getColumn(a);
        var c = newColumn(output);
        int i = 0;
        double total = 0;
        while (i < period) {
            total += as[i];
            i++;
        }
        double avg = total / period;
        Arrays.fill(c.values, 0, i, avg);
        double w = 2d / (period + 1);
        double w2 = 1 - w;
        while (i < length()) {
            avg = w * as[i] + w2 * avg;
            c.values[i] = avg;
            i++;
        }
        return c;
    }

    // add vwap col
    Column vwap(String name) {
        var mids = average("high", "low", "mid");
        var vols = getColumn("volume");
        var c = newColumn(name);
        double cumVol = 0;
        double totalPV = 0;
        var lastDt = dates[0];
        for (int i = 0; i < length(); i++) {
            if (ChronoUnit.MINUTES.between(lastDt, dates[i]) >= 30) {
                cumVol = 0;
                totalPV = 0;
            }

            double vol = vols[i];
            cumVol += vol;
            totalPV += vol * mids.values[i];
            c.values[i] = totalPV / cumVol;
            lastDt = dates[i];
        }
        columns.add(c);
        return c;
    }

    Column rollingMax(String input, int n, String output) {
        return rollingImpl(input, n, output, true);
    }

    Column rollingMin(String input, int n, String output) {
        return rollingImpl(input, n, output, false);
    }

    private Column rollingImpl(String input, int n, String output, boolean isMax) {
        var nums = getColumn(input);
        var c = newColumn(output);
        int[] dq = new int[length()]; // queue holding index of max in window
        int head = 0;       // head will always point to current max/min in window
        int tail = 0;       // insertion point
        int window_trail = 0;
        for (int i = 0; i < length(); i++) {
            if (i >= n) {
                ++window_trail;
            }
            double x = nums[i];
            if (tail > head) {
                // working from back of q remove items which can never be the max/min
                int prev = tail - 1;
                while (prev >= head && (isMax ? x > nums[dq[prev]] : x < nums[dq[prev]])) {
                    --prev;
                }
                tail = prev + 1;
            }
            dq[tail++] = i;
            if (dq[head] < window_trail) {
                ++head;
            }

            c.values[i] = nums[dq[head]];
            // consistency check q must hold decreasing
//            for (int k = head; k < tail - 2; k++) {
//                if (nums[dq[k]] > nums[dq[k + 1]]) {
//                    throw new IllegalStateException("inv dq " + k);
//                }
//            }
        }
        columns.add(c);
        return c;
    }


    /**
     * returns max values in a sliding window of size n
     */
    Column localMax(String input, int n, String output) {
        var nums = getColumn(input);
        var c = newColumn(output);
        int mid = n / 2;
        for (int i = mid; i < length() - mid; ++i) {
            double max = nums[i];
            boolean isMax = true;
            for (int k = i - mid; k <= i + mid; ++k) {
                if (nums[k] > max) {
                    isMax = false;
                    break;
                }
            }
            if (isMax) {
                c.values[i] = max;
            }
        }
        columns.add(c);
        return c;
    }

    /**
     * returns min values in a sliding window of size n
     */
    Column localMin(String input, int n, String output) {
        var nums = getColumn(input);
        var c = newColumn(output);
        int mid = n / 2;
        for (int i = mid; i < length() - mid; ++i) {
            double min = nums[i];
            boolean isMin = true;
            for (int k = i - mid; k <= i + mid; ++k) {
                if (nums[k] < min) {
                    isMin = false;
                    break;
                }
            }
            if (isMin) {
                c.values[i] = min;
            }
        }
        columns.add(c);
        return c;
    }


    public Column hilo(String input, String output) {
        double[] xs = getColumn(input);
        var c = newColumn(output);
        columns.add(c);
        hiloImpl(xs, c.values, 0);
        return c;
    }

    public void hiloImpl(double[] xs, double[] out, int start) {
//        double[] xs = getColumn(input);
//        var c = newColumn(output);
//        double[] out = c.values;
        double last = xs[start];
        for (int i = start + 1; i < size; i++) {
            double x = xs[i];
            if (x > last) {
                int k = i - 1;
                while (k >= 0 && xs[k] < x) {
                    k--;
                }
                out[i] = i - k - 1;
            } else if (x < last) {
                int k = i - 1;
                while (k >= 0 && xs[k] > x) {
                    k--;
                }
                out[i] = k + 1 - i;
            } else {
                out[i] = 0;
            }
            last = x;
        }
    }

    public Column strat(String name) {
        var c = newColumn(name);
        columns.add(c);
        double[] values = c.values;
        var highs = getColumn("high");
        var lows = getColumn("low");
        for (int i = 1; i < length(); i++) {
            int x = highs[i] > highs[i - 1] ? 1 : 0;
            x += lows[i] < lows[i - 1] ? 2 : 0;
            values[i] = x;
        }
        return c;
    }

    private Column newColumn(String output) {
        return new Column(output, max_size);
    }

    public String info() {
        var sb = new StringBuilder();
        for (int i = size - 5; i < size; i++) {
            info(sb, i);
        }
        return sb.toString();
    }

    public void info(StringBuilder sb, int i) {
        if (dates != null) {
            sb.append(dates[i].toLocalTime());
        }
        for (var c : columns) {
            sb.append(String.format(" %.2f", c.values[i]));
        }
        sb.append(System.lineSeparator());
    }

    public void recalc(int n) {
        var lows = getColumn("low");
        var lc = getColumn("lc");
        hiloImpl(lows, lc, n >= 0 ? n : size + n);
        var highs = getColumn("high");
        var hc = getColumn("hc");
        hiloImpl(highs, hc, n >= 0 ? n : size + n);
    }

    public static class Column implements Serializable {
        final String name;
        final double[] values;

        public Column(String name, int size) {
            this.name = name;
            values = new double[size];
        }

        public Column(String name, double[] values) {
            this.name = name;
            this.values = values;
        }

        public Column expand(int newLength) {
            var xs = new double[newLength];
            System.arraycopy(values, 0, xs, 0, values.length);
            return new Column(name, xs);
        }

        @Override
        public String toString() {
            return "Column{" +
                    "name='" + name + '\'' +
                    ", length=" + values.length + '}';
        }
//        public Column(String name, double[] values) {
//            this.name = name;
//            this.values = values;
//        }
    }

    public Bar bar(int i) {
        return new Bar(dates[i], dates[i].plusMinutes(1),
                columns.get(0).values[i],
                columns.get(1).values[i],
                columns.get(2).values[i],
                columns.get(3).values[i],
                columns.get(4).values[i],
                columns.get(5).values[i]);
    }

    public Optional<Bar> bar(LocalDateTime d) {
        int idx = find(d);
        return idx < 0 ? Optional.empty() : Optional.of(bar(idx));
    }

    public String printBars(int n) {
        int start = n >= 0 ? n : n + size;
        int end = Math.min(start + 5, size);
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(bar(i).asIntradayBar()).append(System.lineSeparator());
        }
        return sb.toString();
    }

    public Bar aggregrate(int start, int inclusiveEnd) {
        int s = start >= 0 ? start : length() + start;
        int e = inclusiveEnd >= 0 ? Math.min(inclusiveEnd, length() - 1) : length() + inclusiveEnd;
        if (start > e) {
            throw new IllegalArgumentException("Invalid array bounds " + start + " " + inclusiveEnd);
        }
//        var dates = hist.getDates();
        var opens = getColumn("open");
        var highs = getColumn("high");
        var lows = getColumn("low");
        var closes = getColumn("close");
        var volumes = getColumn("volume");
        var vwaps = getColumn("vwap");
        double high = -1e6;
        double low = 1e6;
        double vol = 0;
        for (int i = s; i <= e; i++) {
            high = Math.max(highs[i], high);
            low = Math.min(lows[i], low);
            vol += volumes[i];
        }
        return new Bar(dates[s], dates[e].plusMinutes(1), opens[s], high, low, closes[e], vol, vwaps[e]);
    }

    public Bar aggregrateDaily(IndexEntry e) {
        return aggregrate(e.start(), e.end());
    }

    public @Nullable Bar aggregrateRth(IndexEntry e) {
        return e.rthStart() >= 0 && e.isComplete() ? aggregrate(e.rthStart(), e.rthEnd()) : null;
    }

    // inclusive end dates
    public record IndexEntry(LocalDate tradeDate, int start, int end, int euStart, int euEnd,
                             int rthStart, int rthEnd, boolean isComplete) {
    }

    public Index index() {
        if (index == null) {
            index = new Index();
        }
        return index;
    }

    public IndexEntry indexEntry(int n) {
        return index().indexEntries.get(n >= 0 ? n : n + index().entries().size());
    }

    List<Bar> dailyBars() {
        return index().indexEntries.stream().map(PriceHistory.this::aggregrateDaily).toList();
    }

    List<Bar> rthBars() {
        // use mapMulti rather than map index -> Optional<Bar> then filter empty -> then map to extract value
        // note either explicit types on lambda required or add type to mapMulti
        return index().indexEntries.stream()
                .<Bar>mapMulti((e, consumer) -> {
                    var b = aggregrateRth(e);
                    if (b != null) consumer.accept(b);
                })
                .toList();
    }

    List<Bar> minVolBars(double minVol) {
        var vols = getColumn("volume");
        var lastBars = lastBars();
        double v = 0;
        int start = 0;
        List<Bar> bars = new ArrayList<>();
        for (int i = 0; i < length(); i++) {
            v += vols[i];
            if (isLastBar(i, lastBars) || v >= minVol) {
                bars.add(aggregrate(start, i));
                start = i + 1;
                v = 0;
            }
        }
        return bars;
    }

    /**
     * Remove head of q if it matches i
     *
     * @return true if i was the head of the queue
     */
    private boolean isLastBar(int i, Queue<Integer> lastBars) {
        boolean f = !lastBars.isEmpty() && i == lastBars.peek();
        if (f) {
            lastBars.remove();
        }
        return f;
    }

    Queue<Integer> lastBars() {
        Queue<Integer> q = new ArrayDeque<>();
        for (IndexEntry entry : index().indexEntries) {
            int i = entry.euEnd();
            if (i > 0) q.add(i);
            i = entry.end();
            if (i > 0) q.add(i);
        }
        return q;
    }

    int[] firstBars(boolean first, int minGap) {
        var idx = new int[128];
        int c = 0;
        LocalDateTime last = null;
        for (int i = 0; i < length(); i++) {
            var date = dates[i];
            if ((last != null && ChronoUnit.MINUTES.between(last, date) > minGap) ||
                    (last == null && first)) {
                idx[c++] = i;
            }
            last = date;
        }
        return Arrays.copyOf(idx, c);
    }

    int[] lastBars(int[] idxs) {
        int len = idxs.length;
        var ends = Arrays.copyOfRange(idxs, 1, len + 1);
        ends[len - 1] = length();
        for (int i = 0; i < ends.length; i++) {
            ends[i] = ends[i] - 1;
        }
        return ends;
    }


    public String debugPrint(int n) {
        int start = 0;
        int end = n;
        if (n < 0) {
            start = length() + n;
            end = length();
        }
        return debugPrint(start, end);
    }


    public String debugPrint(int start, int end) {
        var sb = new StringBuilder();
        var dates = getDates();
        var opens = getColumn("open");
        var highs = getColumn("high");
        var lows = getColumn("low");
        var closes = getColumn("close");
        var volumes = getColumn("volume");
        var vwaps = findColumn("vwap");
        var strats = findColumn("strat");
//        var maxs = getColumn("highm5")
        for (int i = start; i < end; i++) {
            if (vwaps == null) {
                sb.append(String.format("%s %.2f %.2f %.2f %.2f %.0f", dates[i].toLocalTime(), opens[i], highs[i], lows[i], closes[i], volumes[i])).append(System.lineSeparator());
            } else {
                sb.append(String.format("%s %.2f %.2f %.2f %.2f %.0f %.2f %.0f", dates[i].toLocalTime(), opens[i], highs[i], lows[i], closes[i], volumes[i], vwaps[i], strats[i])).append(System.lineSeparator());
            }
        }
        return sb.toString();
    }


    @Override
    public String toString() {
        return dates == null
                ? "PriceHistory[symbol=" + symbol + ", size=" + length() + "]"
                : "PriceHistory[symbol=" + symbol +
                ", size=" + length() +
                ", from=" + dates[0] +
                ", to=" + dates[length() - 1] + "]";
    }

    public class Index {
        List<PriceHistory.IndexEntry> indexEntries = new ArrayList<>();

        private Index() {
            LocalDateTime[] dates = PriceHistory.this.getDates();
            var starts = PriceHistory.this.firstBars(true, 1);
            var ends = PriceHistory.this.lastBars(starts);
            for (int i = 0; i < starts.length; i++) {
                indexEntries.add(createIndexEntry(dates, starts[i], ends[i]));
            }
        }

        private IndexEntry createIndexEntry(LocalDateTime[] dates, int start, int endInclusive) {
            // eu start +9:00 +540 rth start +15:30 +930 from glbx open of 23:00
            int euStart = start + 540 < endInclusive ? start + 540 : -1;
            int rthStart = start + 930 < endInclusive ? start + 930 : -1;
            int rthEnd = start + 1319 < endInclusive ? start + 1319 : -1;
            return new IndexEntry(
                    rthStart > 0 ? dates[rthStart].toLocalDate() : dates[start].toLocalDate().plusDays(1),
                    start,
                    endInclusive,
                    euStart,
                    rthStart > 0 ? rthStart - 1 : -1,
                    rthStart,
                    rthEnd,
                    rthEnd > 0);
        }

        public NavigableMap<Long, String> makeMessages(IndexEntry idx) {
            return makeMessages(idx, null);
        }

        public NavigableMap<Long, String> makeMessages(IndexEntry idx, IndexEntry prev) {
            NavigableMap<Long, String> priceMessages = new TreeMap<>();

            var b = aggregrate(idx.start(), idx.end());
            putMessage(priceMessages, b.open(), "%.2f glbx open");
            putMessage(priceMessages, b.close(), "%.2f last (" + b.end().format(DateTimeFormatter.ofPattern("HH:mm")) + ")");
            var c = PriceHistory.this.getColumn("vwap");
            putMessage(priceMessages, c[idx.end()], "%.2f vwap");

            var glbx = aggregrate(idx.start(), idx.euEnd());
            putMessage(priceMessages, glbx.high(), "%.2f glbx hi");
            putMessage(priceMessages, glbx.low(), "%.2f glbx lo");

            if (idx.euStart() >= 0) {
                var eu = aggregrate(idx.euStart(), idx.euEnd());
                putMessage(priceMessages, eu.open(), "%.2f eu open");
            }

            if (idx.rthStart() >= 0) {
                var rth = aggregrate(idx.rthStart(), idx.rthEnd());
                putMessage(priceMessages, rth.open(), "%.2f open");
                putMessage(priceMessages, rth.high(), "%.2f high");
                putMessage(priceMessages, rth.low(), "%.2f low");
                var rthFirstHour = aggregrate(idx.rthStart(), idx.rthStart() + 59);
                putMessage(priceMessages, rthFirstHour.high(), "%.2f H1 hi");
                putMessage(priceMessages, rthFirstHour.low(), "%.2f H1 lo");
            }

            if (prev != null && prev.rthStart() >= 0) {
                var rth = aggregrate(prev.rthStart(), prev.rthEnd());
                putMessage(priceMessages, rth.high(), "%.2f yh");
                putMessage(priceMessages, rth.low(), "%.2f yl");
                putMessage(priceMessages, rth.close(), "%.2f yc");
            }
            return priceMessages;
        }

        // add message for price level, concatenate with any existing message for same price
        private String putMessage(NavigableMap<Long, String> map, double price, String fmt) {
            String msg = String.format(fmt, price);
            long key = Math.round(price * 100);
            String existing = map.get(key);
            String value = (existing == null) ? msg : existing + ", " + stripWord(msg);
            map.put(key, value);
            return existing;
        }

        private String stripWord(String s) {
            int i = s.indexOf(' ');
            if (i > 0) {
                return s.substring(i + 1);
            }
            return s;
        }

        public String logIntradayInfo(NavigableMap<Long, String> map) {
            var sb = new StringBuilder();
            for (String s : map.reversed().values()) {
                sb.append(System.lineSeparator()).append(s);
            }
            return sb.toString();
        }

        List<IndexEntry> entries() {
            return Collections.unmodifiableList(indexEntries);
        }

        LocalDate rthStartDate(IndexEntry e) {
            if (!e.isComplete) {
                throw new IllegalStateException("Index does not have rth start " + e);
            }
            return PriceHistory.this.getDates()[e.rthStart].toLocalDate();
        }
    }

    public record Bar(LocalDateTime start, LocalDateTime end, double open, double high, double low,
                      double close, double volume, double vwap) {

        public static Bar fromRealTimeBar(RealTimeBar bar) {
            return new PriceHistory.Bar(bar.dt(), bar.dt(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume(), bar.wap());
        }

        public Bar mergeRealTimeBar(RealTimeBar bar) {
            return new PriceHistory.Bar(start(), bar.dt(),
                    open(),
                    Math.max(high(), bar.high()),
                    Math.min(low(), bar.low()),
                    bar.close(),
                    volume() + bar.volume(), bar.wap());
        }

        public String asDailyBar() {
            return "%s, %.2f, %.2f, %.2f, %.2f, %.0f, %.2f".formatted(end.toLocalDate(), open, high, low, close, volume, vwap);
        }

        public String asIntradayBar() {
            return "%s, %.2f, %.2f, %.2f, %.2f, %.0f, %.2f".formatted(start.toLocalTime(), open, high, low, close, volume, vwap);
        }
    }

}
