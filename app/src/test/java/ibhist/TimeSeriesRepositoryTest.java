package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

class TimeSeriesRepositoryTest {
    public static final String CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String DATABASE_NAME = "futures";
    public static final String COLLECTION_NAME = "m1";
    private static final Logger log = LogManager.getLogger("TimeSeriesRepositoryTest");
    private static TimeSeriesRepository repository;

    @BeforeAll
    static void init() {
        repository = new TimeSeriesRepository(CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME);
    }

    @AfterAll
    static void cleanUp() {
        repository.close();
    }

    @Test
    void test_summary() {
        log.info(repository.summary());
        var days = repository.queryDayStartTimes("esz3");
        var days2 = repository.queryDaysWithVolume("esz3", 100_000);
        var history = repository.loadSingleDay("esz3", days.get(days.size() - 1));
        log.info(history);
        log.info(history.index().entries().get(0));
    }

    @Test
    void test_newCollectionLoadAll() {
        var xs = List.of("esh3", "esm3", "esu3", "esz3", "nqz3");
        repository.newTimeSeriesCollection();
        var phr = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        for (String s : xs) {
            var history = phr.load(s);
            history.vwap("vwap");
            repository.insert(history);
        }
    }
}