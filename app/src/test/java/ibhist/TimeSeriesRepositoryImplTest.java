package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
    void test_summary() {
        String symbol = "esu5";
        var summary = repository.summary();
        log.info(summary);
        var startTimes = repository.queryDayStartTimes(symbol);
        var days2 = repository.queryDaysWithVolume(symbol, 100_000);
        var history = repository.loadSingleDay(symbol, startTimes.getFirst());
        log.info(history);
        log.info(history.indexEntry(0));
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