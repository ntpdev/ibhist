package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class PriceHistory implements Serializable {
    private static final Logger log = LogManager.getLogger("PriceHistory");

    LocalDateTime[] dates;
    List<Column> columns = new ArrayList<>();

    PriceHistory(int size, String... names) {
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

    public void insert(int index, LocalDateTime date, double open, double high, double low, double close, int volume) {
        dates[index] = date;
        columns.get(0).values[index] = open;
        columns.get(1).values[index] = high;
        columns.get(2).values[index] = low;
        columns.get(3).values[index] = close;
        columns.get(4).values[index] = volume;
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
    Column vwap(String output) {
        var highs = getColumn("high");
        var lows = getColumn("low");
        var vols = getColumn("volume");
        var c = new Column("vwap", length());
        double cumVol = 0;
        double totalVwap = 0;
        var lastDt = dates[0];
        for (int i = 0; i < length(); i++) {
            if (ChronoUnit.MINUTES.between(lastDt, dates[i]) >= 30) {
                cumVol = 0;
                totalVwap = 0;
            }
            cumVol += vols[i];
            totalVwap += vols[i] * (highs[i] + lows[i]) * .5d;
            c.values[i] = totalVwap / cumVol;
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

    // inclusive end dates
    public record IndexEntry(LocalDate tradeDate, int start, int end, int euStart, int euEnd,
                             int rthStart, int rthEnd, boolean isComplete) {
    }

    public Index makeIndex() {
        return new Index();
    }

    public class Index {
        List<PriceHistory.IndexEntry> indexEntries = new ArrayList<>();

        private Index() {
            LocalDateTime[] dates = PriceHistory.this.getDates();
            int start = 0;

            int lastIdx = dates.length - 1;
            for (int i = 0; i < dates.length; i++) {
                var nextDt = i == lastIdx ? null : dates[i + 1];
                if (nextDt == null || ChronoUnit.MINUTES.between(dates[i], nextDt) >= 30) {
                    indexEntries.add(createIndexEntry(dates, start, i));
                    start = i;
                }
            }
            makeMessages(indexEntries.get(0));
        }

        public NavigableMap<Long, String> makeMessages(IndexEntry e) {
            NavigableMap<Long, String> priceMessages = new TreeMap<>();
            for (IndexEntry idx : indexEntries) {
                var b = PriceHistory.Bar.aggregrate(PriceHistory.this, idx.start(), idx.end());
                putMessage(priceMessages, b.open(), "%.2f glbx open");
                putMessage(priceMessages, b.close(), "%.2f last");
                var c = PriceHistory.this.getColumn("vwap");
                putMessage(priceMessages, c[idx.end()], "%.2f vwap");

                var glbx = PriceHistory.Bar.aggregrate(PriceHistory.this, idx.start(), idx.euEnd());
                putMessage(priceMessages, glbx.high(), "%.2f glbx hi");
                putMessage(priceMessages, glbx.low(), "%.2f glbx lo");

                if (idx.euStart() >= 0) {
                    var eu = PriceHistory.Bar.aggregrate(PriceHistory.this, idx.euStart(), idx.euEnd());
                    putMessage(priceMessages, eu.open(), "%.2f eu open");
                }

                if (idx.rthStart() >= 0) {
                    var rth = PriceHistory.Bar.aggregrate(PriceHistory.this, idx.rthStart(), idx.rthEnd());
                    putMessage(priceMessages, rth.open(), "%.2f open");
                    putMessage(priceMessages, rth.high(), "%.2f high");
                    putMessage(priceMessages, rth.low(), "%.2f low");
                    var rthFirstHour = PriceHistory.Bar.aggregrate(PriceHistory.this, idx.rthStart(), idx.rthStart() + 59);
                    putMessage(priceMessages, rthFirstHour.high(), "%.2f H1 hi");
                    putMessage(priceMessages, rthFirstHour.low(), "%.2f H1 lo");
                }
                logIntradayInfo(priceMessages);
                return priceMessages;
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

        private void logIntradayInfo(NavigableMap<Long, String> map) {
            //TODO can replace with JDK21 SequencedMap reversed().values()
            for (String s : map.descendingMap().values()) {
                log.info(s);
            }
        }

        private IndexEntry createIndexEntry(LocalDateTime[] dates, int start, int endInclusive) {
            // eu start +9:00 +540 rth start +15:30 +930 from glbx open of 23:00
            int euStart = start + 540 < dates.length ? start + 540 : -1;
            int rthStart = start + 930 < dates.length ? start + 930 : -1;
            int rthEnd = start + 1319 < dates.length ? start + 1319 : -1;
            return new IndexEntry(
                    rthStart > 0 ? dates[rthStart].toLocalDate() : dates[start].toLocalDate().plusDays(1),
                    start,
                    endInclusive,
                    euStart,
                    rthStart > 0 ? rthStart - 1 : -1, rthStart,
                    rthEnd,
                    rthEnd > 0);
        }

        List<IndexEntry> getIndexEntries() {
            return Collections.unmodifiableList(indexEntries);
        }

        LocalDate rthStartDate(IndexEntry e) {
            if (!e.isComplete) {
                throw new IllegalStateException("Index does not have rth start " + e);
            }
            return PriceHistory.this.getDates()[e.rthStart].toLocalDate();
        }
    }

    public record Bar(LocalDateTime start, LocalDateTime end, double open, double high, double low, double close,
                      double volume) {
        public static Bar aggregrate(PriceHistory hist, int start, int inclusiveEnd) {
            int end = inclusiveEnd >= 0 ? Math.min(inclusiveEnd, hist.length() - 1) : hist.length() + inclusiveEnd;
            if (start > end) {
                throw new IllegalArgumentException("Invalid array bounds " + start + " " + inclusiveEnd);
            }
            var dates = hist.getDates();
            var opens = hist.getColumn("open");
            var highs = hist.getColumn("high");
            var lows = hist.getColumn("low");
            var closes = hist.getColumn("close");
            var volumes = hist.getColumn("volume");
            double high = -1e6;
            double low = 1e6;
            double vol = 0;
            for (int i = start; i <= end; i++) {
                high = Math.max(highs[i], high);
                low = Math.min(lows[i], low);
                vol += volumes[i];
            }
            return new Bar(dates[start], dates[end].plusMinutes(1), opens[start], high, low, closes[end], vol);
        }

    }

}
