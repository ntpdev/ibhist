package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class PriceHistory implements Serializable {
    private static final Logger log = LogManager.getLogger("PriceHistory");

    private final String symbol;
    LocalDateTime[] dates;
    List<Column> columns = new ArrayList<>();
    private transient Index index = null;

    PriceHistory(String symbol, int size, String... names) {
        this.symbol = symbol;
        for (String name : names) {
            if (name.equals("date")) {
                dates = new LocalDateTime[size];
            } else {
                columns.add(new Column(name, size));
            }
        }
    }

    int length() {
        return columns.get(0).values.length;
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
        for (var c : columns) {
            if (c.name.equals(name)) {
                return c.values;
            }
        }
        throw new RuntimeException("column not found " + name);
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

    public void insert(int index, LocalDateTime date, double open, double high, double low, double close, int volume) {
        dates[index] = date;
        columns.get(0).values[index] = open;
        columns.get(1).values[index] = high;
        columns.get(2).values[index] = low;
        columns.get(3).values[index] = close;
        columns.get(4).values[index] = volume;
    }

    public void insert(int index, LocalDateTime date, double open, double high, double low, double close, double volume, double vwap) {
        dates[index] = date;
        columns.get(0).values[index] = open;
        columns.get(1).values[index] = high;
        columns.get(2).values[index] = low;
        columns.get(3).values[index] = close;
        columns.get(4).values[index] = volume;
        columns.get(5).values[index] = vwap;
    }

    Column average(String a, String b, String output) {
        var as = getColumn(a);
        var bs = getColumn(b);
        var c = new Column(output, as.length);
        for (int i = 0; i < as.length; i++) {
            c.values[i] = (as[i] + bs[i]) * .5;
        }
        return c;
    }

    Column cumulative(String a, String output) {
        var as = getColumn(a);
        var c = new Column(output, as.length);
        double cumulative = 0;
        for (int i = 0; i < as.length; i++) {
            cumulative += as[i];
            c.values[i] = cumulative;
        }
        return c;
    }

    Column ema(String a, String output, int period) {
        var as = getColumn(a);
        var c = new Column(output, as.length);
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
        while (i < as.length) {
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
        var c = new Column(name, length());
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

    public static class Column implements Serializable {
        final String name;
        final double[] values;

        public Column(String name, int size) {
            this.name = name;
            values = new double[size];
        }
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

    public Bar aggregrate(int start, int inclusiveEnd) {
        int end = inclusiveEnd >= 0 ? Math.min(inclusiveEnd, length() - 1) : length() + inclusiveEnd;
        if (start > end) {
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
        for (int i = start; i <= end; i++) {
            high = Math.max(highs[i], high);
            low = Math.min(lows[i], low);
            vol += volumes[i];
        }
        return new Bar(dates[start], dates[end].plusMinutes(1), opens[start], high, low, closes[end], vol, vwaps[end]);
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

    List<Bar> dailyBars() {
        return index().indexEntries.stream().map(PriceHistory.this::aggregrateDaily).toList();
    }

    List<Bar> rthBars() {
        // use mapMulti rather than map index -> Optional<Bar> then filter empty -> then map to extract value
        // note either explicit types on lambda required or add type to mapMulti
        return index().indexEntries.stream()
                .<Bar>mapMulti((e, consumer) -> { var b = aggregrateRth(e); if (b != null) consumer.accept(b); })
                .toList();
    }

    List<Bar> minVolBars(double minVol) {
        var vols = getColumn("volume");
        var lastBars = lastBars();
        double v = 0;
        int start = 0;
        List<Bar> bars = new ArrayList<>();
        for (int i = 0; i < vols.length; i++) {
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
        for (int i = 0; i < dates.length; i++) {
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
        var ends = Arrays.copyOfRange(idxs, 1, len+1);
        ends[len -1] = dates.length;
        for (int i = 0; i < ends.length; i++) {
            ends[i] = ends[i] - 1;
        }
        return ends;
    }

    @Override
    public String toString() {
        return "PriceHistory[symbol=" + symbol +
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
            NavigableMap<Long, String> priceMessages = new TreeMap<>();

            var b = aggregrate(idx.start(), idx.end());
            putMessage(priceMessages, b.open(), "%.2f glbx open");
            putMessage(priceMessages, b.close(), "%.2f last");
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
            return priceMessages;
        }
        // add message for price level, concatenate with any existing message for same price

        private String putMessage(NavigableMap<Long, String> map, double price, String fmt) {
            String msg = String.format(fmt, price);
            long key = Math.round(price * 100);
            String existing = map.get(key);
            String value = (existing == null) ? msg : existing + ", " + msg;
            map.put(key, value);
            return existing;
        }

        public String logIntradayInfo(NavigableMap<Long, String> map) {
            var sb = new StringBuilder();
            for (String s : map.reversed().values()) {
                sb.append(s).append(System.lineSeparator());
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
        public String asDailyBar() {
            return String.format("%s, %.2f, %.2f, %.2f, %.2f, %.0f, %.2f", end.toLocalDate(), open, high, low, close, volume, vwap);
        }
    }

}
