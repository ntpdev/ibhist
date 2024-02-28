package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.format.DateTimeFormatter;
import java.util.List;

class TimeSeriesRepositoryImplTest {
    private static final Logger log = LogManager.getLogger("TimeSeriesRepositoryImplTest");
    private static TimeSeriesRepositoryImpl repository;

    @BeforeAll
    static void init() {
        repository = new TimeSeriesRepositoryImpl();
    }

    @AfterAll
    static void cleanUp() {
        repository.close();
    }

    @Test
    void test_summary() {
        log.info(repository.summary());
        var startTimes = repository.queryDayStartTimes("esz3");
        var days2 = repository.queryDaysWithVolume("esz3", 100_000);
        var history = repository.loadSingleDay("esz3", startTimes.getLast());
        log.info(history);
        log.info(history.index().entries().getFirst());
    }

    @Test
    void test_newCollectionLoadAll() {
        var xs = List.of("esh4", "nqh4");
//        var xs = List.of("esh3", "esm3", "esu3", "esz3", "nqz3", "esh4", "nqh4");
        repository.newTimeSeriesCollection();
        var phr = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        for (String s : xs) {
            var history = phr.load(s);
            history.vwap("vwap");
            history.dailyBars();
            repository.insert(history);
        }
    }

    @Test
    void test_print_daily() {
        var phr = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        var history = phr.load("esh4");
        history.vwap("vwap");
//        var sb = new StringBuilder();
        saveDaily("es-daily", history.dailyBars());
        saveDaily("es-daily-rth", history.rthBars());
//        sb.append("--- daily ---");
//        sb.append("Date,Open,High,Low,Close,Volume,VWAP,Change,Gap,DayChg,Range");
//        sb.append(System.lineSeparator());
//        var xs = history.dailyBars();
//        PriceHistory.Bar last = null;
//        for (PriceHistory.Bar bar : xs) {
//            if (bar.volume() > 500_000) {
//                String s = last == null
//                    ? String.format(", 0, 0, %.2f, %.2f", bar.close() - bar.open(), bar.high() - bar.low())
//                    : String.format(", %.2f, %.2f, %.2f, %.2f", bar.close() - last.close(), bar.open() - last.close(), bar.close() - bar.open(), bar.high() - bar.low());
//                sb.append(bar.asDailyBar()).append(s).append(System.lineSeparator());
//            }
//            last = bar;
//        }
//
//        sb.append("--- rth ---");
//        sb.append("Date,Open,High,Low,Close,Volume,VWAP,Change,Gap,DayChg,Range");
//        sb.append(System.lineSeparator());
//        last = null;
//        saveDaily(history.rthBars());
//        for (PriceHistory.Bar bar : history.rthBars()) {
//            if (bar.volume() > 500_000) {
//                String s = last == null
//                        ? String.format(", 0, 0, %.2f, %.2f", bar.close() - bar.open(), bar.high() - bar.low())
//                        : String.format(", %.2f, %.2f, %.2f, %.2f", bar.close() - last.close(), bar.open() - last.close(), bar.close() - bar.open(), bar.high() - bar.low());
//                sb.append(bar.asDailyBar()).append(s).append(System.lineSeparator());
//            }
//            last = bar;
//        }
//
//        log.info(sb.toString());
    }

    private void saveDaily(String fname, List<PriceHistory.Bar> bars) {
        try {
            var sb = new StringBuilder();
            sb.append("Date,Open,High,Low,Close,Volume,VWAP,Change,Gap,DayChg,Range");
            sb.append(System.lineSeparator());
            PriceHistory.Bar last = null;
            for (PriceHistory.Bar bar : bars) {
                if (bar.volume() > 500_000) {
                    String s = last == null
                            ? String.format(", 0, 0, %.2f, %.2f", bar.close() - bar.open(), bar.high() - bar.low())
                            : String.format(", %.2f, %.2f, %.2f, %.2f", bar.close() - last.close(), bar.open() - last.close(), bar.close() - bar.open(), bar.high() - bar.low());
                    sb.append(bar.asDailyBar()).append(s).append(System.lineSeparator());
                }
                last = bar;
            }
            String fn = fname + "-" + bars.getLast().end().format(DateTimeFormatter.BASIC_ISO_DATE) + ".csv";
            log.info("saving " + fn);
            Files.writeString(Path.of("c:\\temp\\ultra", fn), sb.toString(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error(e);
        }
    }

}