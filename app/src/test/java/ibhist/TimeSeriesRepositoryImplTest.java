package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.format.DateTimeFormatter;
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
        log.info(repository.summary());
        var startTimes = repository.queryDayStartTimes("esh5");
        var days2 = repository.queryDaysWithVolume("esh5", 100_000);
        var history = repository.loadSingleDay("esh5", startTimes.getLast());
        log.info(history);
        log.info(history.index().entries().getFirst());
    }

    @Test
    void test_newCollectionLoadAll() {
        var xs = List.of("esh5", "nqh5", "esm4", "nqm4", "esu4", "nqu4", "esz4", "nqz4");
//        var xs = List.of("esh3", "esm3", "esu3", "esz3", "nqz3", "esh4", "nqh4");
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