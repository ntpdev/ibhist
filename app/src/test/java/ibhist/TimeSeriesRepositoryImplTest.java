package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

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
    void test_load_PriceHistory_for_range() {
        String symbol = "esh6";
        var history = repository.loadPriceHistory(symbol, LocalDate.of(2026,1,30), LocalDate.of(2026,2,2), false);
        assertThat(history.getSymbol()).isEqualTo(symbol);
        assertThat(history.length()).isGreaterThan(60);
        assertThat(history.index().entries().size()).isEqualTo(2);
        log.info(history);
        log.info(history.indexEntry(0));
    }

    @Test
    void test_buildIndex() {
        var symbols = repository.buildTradeDateIndex();
        log.info("rebuild trade_date_index for {} symbols", symbols.size());
    }

    @Test
    void test_queryTradeDateIndex() {
        var xs = repository.queryTradeDates("esh6", 100_000);
        log.info("loaded {} rows from {} to {}",  xs.size(), xs.getFirst().date(), xs.getLast().date());
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
        LocalDate sliceStart = daily.get(daily.size() - 5).date();
        LocalDate sliceEnd = daily.getLast().date();
        var rth = repository.queryDailyBars(symbol, sliceStart, sliceEnd, true);
        log.info("--- daily {} ---", symbol);
        for (int i = Math.max(0, daily.size() - 5); i < daily.size(); i++) {
            var bar = daily.get(i);
            log.info(String.format("%s %.2f %.2f %.2f %.2f %7d", bar.date(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume()));
        }
        log.info("--- rth {} ---", symbol);
        for (var bar : rth) {
            log.info(String.format("%s %.2f %.2f %.2f %.2f %7d", bar.date(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume()));
        }
    }

    @Test
    void test_newCollectionLoadAll() {
        var phr = new PriceHistoryRepository();
        var xs = phr.findAllSymbols();
        repository.createM1TimeSeriesCollection("m1", true);
        repository.createMinVolTimeSeriesCollection("min_vol", true);
        xs.stream().map(phr::load).filter(Optional::isPresent).map(Optional::get).forEach(
                h -> {
                    // add vwap and ema indicators to history and insert into m1 and optionally min_vol collections
                    h.vwap("vwap");
                    h.columns.add(h.ema("close", "ema", 87));
                    h.dailyBars();
                    repository.insertM1(h);
                    if (h.getSymbol().startsWith("es")) {
                        repository.insertMinVol(h);
                    }
                });
    }

    @Test
    void test_load_min_vol() {
        var repo = new PriceHistoryRepository();
        var hist = repo.load("ESH6", Paths.get(System.getProperty("user.home"), "Documents", "data", "zESH6 20260206.csv"));
        hist.addStandardColumns();
        log.info(hist);
        repository.createMinVolTimeSeriesCollection("min_vol", true);
        repository.insertMinVol(hist);
    }

    @Test
    void test_rebuild_min_vol() {
        // rebuild last 5 days of minvol from m1
        var hist = repository.loadPriceHistory("esh6", -5, 5);
        repository.rebuildMinVol(hist);
    }
}