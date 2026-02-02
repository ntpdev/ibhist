package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

class TimeSeriesRepositoryImplTest {
    private static final Logger log = LogManager.getLogger(TimeSeriesRepositoryImplTest.class.getSimpleName());
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
    void test_queryM1Summary() {
        String symbol = "esh6";
        var summary = repository.queryM1Summary();
        log.info(summary);
        var startTimes = repository.queryDayStartTimes(symbol);
        var days2 = repository.queryDaysWithVolume(symbol, 100_000);
        var history = repository.loadSingleDay(symbol, startTimes.getFirst());
        log.info(history);
        log.info(history.indexEntry(0));
    }

    @Test
    void test_buildIndex() {
        String symbol = "esh6";
        var summary = repository.buildTradeDateIndex();
    }

    @Test
    void test_queryTradeDateIndex() {
        var xs = repository.queryTradeDates("esh6", 100_000);
        log.info("loaded {} rows from {} to {}",  xs.size(), xs.getFirst().tradeDate(), xs.getLast().tradeDate());
        log.info(xs.getLast());
    }

    @Test
    void test_buildDailyBars() {
        repository.buildAllDailyTimeSeries();
    }

    @Test
    void test_queryDailyBars() {
        String symbol = "esh6";
        var daily = repository.queryDailyBars(symbol, false);
        LocalDate sliceStart = daily.get(daily.size() - 5).tradeDate();
        LocalDate sliceEnd = daily.getLast().tradeDate();
        var rth = repository.queryDailyBars(symbol, sliceStart, sliceEnd, true);
        log.info("--- daily {} ---", symbol);
        for (int i = Math.max(0, daily.size() - 5); i < daily.size(); i++) {
            var bar = daily.get(i);
            log.info(String.format("%s %.2f %.2f %.2f %.2f %7d", bar.tradeDate(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume()));
        }
        log.info("--- rth {} ---", symbol);
        for (var bar : rth) {
            log.info(String.format("%s %.2f %.2f %.2f %.2f %7d", bar.tradeDate(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume()));
        }
    }

    @Test
    void test_newCollectionLoadAll() {
        var xs = List.of("esh6", "nqh6","esz5", "nqz5","esu5", "nqu5", "esm5", "nqm5", "esh5", "nqh5", "esm4", "nqm4", "esu4", "nqu4", "esz4", "nqz4");
        repository.newTimeSeriesCollection();
        var phr = new PriceHistoryRepository();
        xs.stream().map(phr::load).filter(Optional::isPresent).map(Optional::get).forEach(
                h -> {
                    h.vwap("vwap");
                    h.dailyBars();
                    repository.insert(h);
                });
    }
}