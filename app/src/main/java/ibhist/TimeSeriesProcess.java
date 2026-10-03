package ibhist;

import com.google.inject.Inject;
import ibhist.TimeSeriesRepository.DailyBar;
import ibhist.TimeSeriesRepository.TradeDateIndexEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Production entry point for building and rebuilding the MongoDB time-series collections
 * from the CSV price history repository.
 *
 * <p>{@link #rebuild()} reconstructs {@code m1} and {@code min_vol} from scratch for all
 * ES/NQ futures and then rebuilds the trade-date index and daily series. The remaining
 * methods expose the individual load/query/rebuild steps as reusable production
 * operations; none of them are wired to the CLI.
 */
public class TimeSeriesProcess {
    private static final Logger log = LogManager.getLogger(TimeSeriesProcess.class.getSimpleName());
    private static final List<String> SYMBOL_PREFIXES = List.of("es", "nq");

    private final TimeSeriesRepository timeSeriesRepo;
    private final PriceHistoryRepository priceHistoryRepo;

    @Inject
    public TimeSeriesProcess(TimeSeriesRepository timeSeriesRepo, PriceHistoryRepository priceHistoryRepo) {
        this.timeSeriesRepo = timeSeriesRepo;
        this.priceHistoryRepo = priceHistoryRepo;
    }

    /**
     * Rebuilds all time-series collections from the CSV source of truth.
     */
    public void rebuild() {
        var symbols = priceHistoryRepo.findAllFuturesSymbols().stream()
                .filter(s -> SYMBOL_PREFIXES.stream().anyMatch(s::startsWith))
                .toList();

        if (symbols.isEmpty()) {
            log.info("no futures symbols found for prefixes {}", SYMBOL_PREFIXES);
            return;
        }

        log.info("rebuilding time series for {} symbols {}", symbols.size(), symbols);
        timeSeriesRepo.createM1TimeSeriesCollection(TimeSeriesRepositoryImpl.M1_COLLECTION, true);
        timeSeriesRepo.createMinVolTimeSeriesCollection(TimeSeriesRepositoryImpl.MIN_VOL_COLLECTION, true);

        symbols.stream()
                .map(priceHistoryRepo::load)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(history -> {
                    timeSeriesRepo.insertM1(history);
                    timeSeriesRepo.insertMinVol(history);
                });

        timeSeriesRepo.rebuildTradeDateIndex();
        timeSeriesRepo.buildAllDaily();
        log.info("rebuild complete");
    }

    /**
     * Load and log the price history covering the inclusive range of trade dates.
     */
    public PriceHistory loadPriceHistoryForRange(String symbol, LocalDate startDate, LocalDate endDate, boolean rthOnly) {
        var history = timeSeriesRepo.loadPriceHistory(symbol, startDate, endDate, rthOnly);
        log.info("loaded {} bars for {} ({} to {}, rth={})", history.length(), symbol, startDate, endDate, rthOnly);
        log.info(history);
        log.info(history.indexEntry(0));
        return history;
    }

    /**
     * Rebuild the trade_date_index collection for all symbols from m1 data.
     */
    public List<String> buildTradeDateIndex() {
        var symbols = timeSeriesRepo.rebuildTradeDateIndex();
        log.info("rebuild trade_date_index for {} symbols", symbols.size());
        return symbols;
    }

    /**
     * Query the trade_date_index for a symbol, keeping only entries with at least {@code minVolume}.
     */
    public List<TradeDateIndexEntry> queryTradeDates(String symbol, long minVolume) {
        var xs = timeSeriesRepo.queryTradeDates(symbol, minVolume);
        if (!xs.isEmpty()) {
            log.info("loaded {} rows from {} to {}", xs.size(), xs.getFirst().date(), xs.getLast().date());
            log.info(xs.getLast());
        }
        return xs;
    }

    /**
     * Rebuild all daily and dailyRTH time series from m1 data.
     */
    public void buildDailyBars() {
        timeSeriesRepo.buildAllDaily();
    }

    /**
     * Query and log the most recent full-session and RTH daily bars for a symbol.
     */
    public List<DailyBar> queryDailyBars(String symbol) {
        var daily = timeSeriesRepo.queryDailyBars(symbol, false);
        if (daily.isEmpty()) {
            return daily;
        }

        int from = Math.max(0, daily.size() - 5);
        var sliceStart = daily.get(from).date();
        var sliceEnd = daily.getLast().date();
        var rth = timeSeriesRepo.queryDailyBars(symbol, sliceStart, sliceEnd, true);

        log.info("--- daily {} ---", symbol);
        for (int i = from; i < daily.size(); i++) {
            logDailyBar(daily.get(i));
        }
        log.info("--- rth {} ---", symbol);
        rth.forEach(this::logDailyBar);
        return daily;
    }

    /**
     * Selectively rebuild m1, trade_date_index, min_vol and daily for the trade dates in a CSV file.
     */
    public PriceHistory selectiveRebuild(String symbol, Path file) {
        var history = priceHistoryRepo.load(symbol, file);
        history.addStandardColumns();
        log.info(history);
        timeSeriesRepo.selectiveRebuild(history);
        return history;
    }

    /**
     * Rebuild the last {@code days} of min_vol for a symbol from m1 data.
     */
    public PriceHistory rebuildMinVol(String symbol, int index, int days) {
        var history = timeSeriesRepo.loadPriceHistory(symbol, index, days);
        timeSeriesRepo.rebuildMinVol(history);
        timeSeriesRepo.insertMinVol(history);
        return history;
    }

    /**
     * Rebuild daily and dailyRTH time series for the trade dates in a CSV file.
     */
    public PriceHistory rebuildDaily(String symbol, Path file) {
        var history = priceHistoryRepo.load(symbol, file);
        timeSeriesRepo.rebuildDaily(history);
        return history;
    }

    private void logDailyBar(DailyBar bar) {
        log.info(String.format("%s %.2f %.2f %.2f %.2f %7d", bar.date(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume()));
    }
}
