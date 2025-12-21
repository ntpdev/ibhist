package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.ib.client.Bar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PriceHistoryRepository {

    private static final Logger log = LogManager.getLogger(PriceHistoryRepository.class.getSimpleName());
    // will use Guava splitter rather than regex \s+
    static final Splitter COMMA_SPLITTER = Splitter.on(",").trimResults();
    static final Splitter WS_SPLITTER = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings();

    private final Path root;
    private final String extension;

    PriceHistoryRepository() {
        this(Paths.get(System.getProperty("user.home"), "Documents", "data"), ".csv");
    }

    PriceHistoryRepository(Path root, String extension) {
        this.root = root;
        this.extension = extension;
    }

    Optional<PriceHistory> load(String symbol) {
        return load(symbol, true);
    }

    Optional<PriceHistory> load(String symbol, boolean useCache) {
        try {
            Path cacheFile = root.resolve(symbol + ".bin");
            var priceHistory = useCache && existsRecent(cacheFile, 60) ? loadFromCache(cacheFile) : loadAndCache(symbol, cacheFile);
            log.info("file loaded {}", priceHistory);
            return priceHistory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean existsRecent(Path path, long ageMinutes) throws IOException {
        if (!Files.exists(path)) {
            return false;
        }
        FileTime lastModifiedTime = Files.getLastModifiedTime(path);
        return lastModifiedTime.toInstant().isAfter(Instant.now().minusSeconds(60 * ageMinutes));
    }

    private Optional<PriceHistory> loadAndCache(String symbol, Path cacheFile) throws IOException {
        var priceHistory = loadImpl(symbol);
        priceHistory.ifPresent(history -> saveToCache(cacheFile, history));
        return priceHistory;
    }

    private Optional<PriceHistory> loadImpl(String symbol) throws IOException {
        var parsedCsv = loadCsv(findFiles(symbol));
        if (!parsedCsv.isEmpty()) {
            PriceHistory priceHistory = new PriceHistory(symbol, parsedCsv.size(), "date", "open", "high", "low", "close", "volume");
            int i = 0;
            LocalDateTime lastBar = null;
            for (IBCSV line : parsedCsv) {
                if (lastBar != null) {
                    long diff = ChronoUnit.MINUTES.between(lastBar, line.date());
                    if (diff <= 0) {
                        throw new RuntimeException("Current bar " + line.date() + " is before last bar " + lastBar);
                    }
                    if (diff > 1) {
                        log.info("Gap between {} and {} of {} minutes", lastBar, line.date(), diff);
                    }
                }
                priceHistory.insert(i++, line.date(), line.open().doubleValue(), line.high().doubleValue(), line.low().doubleValue(), line.close().doubleValue(), line.volume());
                lastBar = line.date();
            }
            return Optional.of(priceHistory);
        }
        return Optional.empty();
    }

    PriceHistory load(String symbol, Path file) {
        try {
            var parsedCsv = loadCsv(List.of(file));
            PriceHistory priceHistory = new PriceHistory(symbol, parsedCsv.size(), "date", "open", "high", "low", "close", "volume");
            int i = 0;
            for (var line : parsedCsv) {
                priceHistory.insert(i++, line.date(), line.open().doubleValue(), line.high().doubleValue(), line.low().doubleValue(), line.close().doubleValue(), line.volume());
            }
            return priceHistory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Optional<PriceHistory> loadFromCache(Path cacheFile) {
        log.info("Loading from cache file {}", cacheFile);
        try (ObjectInputStream objectOutput = new ObjectInputStream(
                new GZIPInputStream(new FileInputStream(cacheFile.toFile())));) {
            return Optional.of((PriceHistory) objectOutput.readObject());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    void saveToCache(Path cacheFile, PriceHistory priceHistory) {
        log.info("Saving to cache file {}", cacheFile);
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(
                new GZIPOutputStream(new FileOutputStream(cacheFile.toFile())))) {
            objectOutput.writeObject(priceHistory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    List<Path> findFiles(String symbol) throws IOException {
        return Files.list(root)
                .filter(e -> matches(e, symbol))
                .sorted()
                .toList();
    }

    boolean matches(Path path, String prefix) {
        String fname = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return fname.startsWith(prefix) && fname.endsWith(extension);
    }

    /**
     * load csv files in order. duplicate timestamps are skipped
     * @param files
     * @return
     * @throws IOException
     */
    List<IBCSV> loadCsv(List<Path> files) throws IOException {
        List<IBCSV> xs = new ArrayList<>();
        for (var file : files) {
            LocalDateTime hw = xs.isEmpty()
                    ? LocalDateTime.of(2020, 1, 1, 0, 0)
                    : xs.getLast().date();
            log.info("loadCsv {}", file);
            // skip header line which does not have an index
            xs.addAll(Files.lines(file)
                    .filter(e -> Character.isDigit(e.charAt(0)))
                    .map(IBCSV::parse)
                    .filter(e -> e.date().isAfter(hw))
                    .toList());
        }
        return xs;
    }

    record IBCSV(LocalDateTime date, BigDecimal open, BigDecimal high,
                 BigDecimal low, BigDecimal close, int volume, BigDecimal wap) {

        //0,20221204  23:00:00,4102.5,4107.25,4102.5,4105.0,34,4103.65,13
        static IBCSV parse(String s) {
            var xs = COMMA_SPLITTER.splitToList(s);
            var ds = WS_SPLITTER.splitToList(xs.get(1));
            var d = LocalDateTime.of(
                    LocalDate.parse(ds.get(0), DateTimeFormatter.BASIC_ISO_DATE),
                    LocalTime.parse(ds.get(1), DateTimeFormatter.ISO_LOCAL_TIME));
            return new IBCSV(d,
                    new BigDecimal(xs.get(2)).setScale(2, RoundingMode.HALF_UP),
                    new BigDecimal(xs.get(3)).setScale(2, RoundingMode.HALF_UP),
                    new BigDecimal(xs.get(4)).setScale(2, RoundingMode.HALF_UP),
                    new BigDecimal(xs.get(5)).setScale(2, RoundingMode.HALF_UP),
                    Integer.parseInt(xs.get(6)),
                    new BigDecimal(xs.get(7)).setScale(3, RoundingMode.HALF_UP));
            // ignore bar threshold
        }
    }

}
