package ibhist;

import com.ib.client.Bar;
import com.ib.client.Decimal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PriceHistoryRepositoryTest {
    private static final Logger log = LogManager.getLogger("PriceHistoryRepositoryTest");

    @Test
    void test_loadAllDays() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        var history = repository.load("esh4");
        var c = history.vwap("vwap");
        var cMax = history.rollingMax("high", 5, "highm5");
        assertThat(c.values.length).isEqualTo(history.length());
        var entry = history.indexEntry(-2);
        LocalDateTime date = history.getDates()[entry.start()];
        int pos = history.find(date);
        assertThat(pos).isEqualTo(entry.start());
        var b = history.bar(pos);
        pos = history.find(date.minusMinutes(1)); // no exact match so returns -ve
        assertThat(pos).isLessThan(0);
        pos = history.floor(date.minusMinutes(1));
        assertThat(pos).isEqualTo(entry.start() - 1);
//        TimeSeriesRepository tsr = new TimeSeriesRepository("mongodb://localhost:27017");
//        tsr.append(history);
        var rthBars = history.rthBars();
        for (PriceHistory.Bar bar : rthBars) {
            log.info(bar.asDailyBar());
        }
        var minVol = history.minVolBars(2500d);
        save(minVol);
    }

    @Test
    void test_expandingWindow() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        var history = repository.load("esh4");
        var vwap = history.vwap("vwap");
        var entry = history.indexEntry(-4);

        var highs = history.getColumn("high");
        var lows = history.getColumn("low");
        var highPoints = new ArrayList<Point>();
        var lowPoints = new ArrayList<Point>();
        double hiwm = -1e6;
        double lowm = 1e6;
        for (int i = entry.rthEnd(); i <= entry.rthEnd() + 900; i++) {
            double h = highs[i];
            double l = lows[i];
            if (h > hiwm) {
                int c = 0;
                highPoints.add(new Point(i, h, c));
                hiwm = h;
            }
            if (l < lowm) {
                int c = 0;
                lowPoints.add(new Point(i, l, c));
                lowm = l;
            }
        }
        Point last = null;
        for (var p : highPoints) {
            if (last != null) {
                log.info(String.format("%s %d %d %.2f", history.dates[p.index].toLocalTime(), p.index, p.index - last.index, p.value));
            }
            last = p;
        }
    }

    @Test
    void test_expandingWindowReversed() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        var history = repository.load("esh4");
        var vwap = history.vwap("vwap");
        var entry = history.indexEntry(-1);

        var highs = history.getColumn("high");
        var lows = history.getColumn("low");
        var highPoints = new ArrayList<Point>();
        var lowPoints = new ArrayList<Point>();
        double hiwm = -1e6;
        double lowm = 1e6;
        for (int i = entry.rthStart() + 260; i >= entry.rthStart(); i--) {
            double h = highs[i];
            double l = lows[i];
            if (h > hiwm) {
                int c = highPoints.isEmpty() ? 0 : i - highPoints.getFirst().index;
                highPoints.add(new Point(i, h, c));
                hiwm = h;
            }
            if (l < lowm) {
                int c = lowPoints.isEmpty() ? 0 : i - lowPoints.getFirst().index;
                lowPoints.add(new Point(i, l, c));
                lowm = l;
            }
        }
        Point last = null;
        for (var p : highPoints) {
            if (last != null) {
                if (p.count > 19)
                    log.info("over %.2f".formatted(last.value));
                log.info("%s %s".formatted(history.dates[p.index].toLocalTime(), p));
            }
            last = p;
        }
        log.info(history.printBars(highPoints.getLast().index));
    }

    record Point(int index, double value, int count) {
    }

    @Test
    void test_loadSingleDay() {
        try {
            var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
            var xs = repository.list("zesm4");
            for (Path p : xs) {
                var history = repository.load("zESM4", p);
                history.vwap("vwap");
                System.out.println(history);
                var bars = history.rthBars();
                for (PriceHistory.Bar bar : bars) {
                    System.out.println(bar.asDailyBar());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void save(List<PriceHistory.Bar> minVol) {
        try {
            var buffer = new StringBuffer();
            buffer.append(",Date,DateCl,Open,High,Low,Close,Volume,VWAP").append(System.lineSeparator());
            for (int i = 0; i < minVol.size(); i++) {
                PriceHistory.Bar bar = minVol.get(i);
                buffer.append("%d,%s,%s,%.2f,%.2f,%.2f,%.2f,%.1f,%.2f%n".formatted(i, bar.start(), bar.end(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume(), bar.vwap()));
            }
            Files.writeString(Path.of("c:\\temp\\ultra\\es-minvol.csv"), buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void test_load_ib_bars() {
        List<Bar> bars = List.of(
                new Bar("20230919 23:00:00 Europe/London", 21.25, 25.75, 19.00, 22.50, Decimal.get(123.45d), 13, Decimal.get(23.45d)));
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");

        var history = repository.loadFromIBBars("test", bars);

        assertThat(history.length()).isEqualTo(1);
        assertThat(history.getDates()[0]).isEqualTo(LocalDateTime.of(2023, 9, 19, 23, 0, 0));
    }
}