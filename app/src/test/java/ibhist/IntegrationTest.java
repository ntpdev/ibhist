package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public class IntegrationTest {
    private static final Logger log = LogManager.getLogger(IntegrationTest.class.getSimpleName());

    @Test
    void test() {
        var repo = new PriceHistoryRepositoryImpl();
        var hist = repo.load("esm6", Paths.get(System.getProperty("user.home"), "Documents", "data", "zESM6 20260313.csv"));
        hist.addStandardColumns();
        log.info(hist);
        var idx = hist.indexEntry(LocalDate.of(2026, 3, 18));
        log.info(idx);
        var xs = ArrayUtils.findSwings(hist.getColumn("high"), hist.getColumn("low"), idx.start(), idx.end(), 5);
        log.info("Swings: {}", xs);
    }
}
