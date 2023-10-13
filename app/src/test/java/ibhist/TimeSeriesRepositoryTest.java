package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
        var history = repository.loadSingleDay("esz3", days.get(days.size() - 1));
        log.info(history);
        log.info(history.index().entries().get(0));
    }
}