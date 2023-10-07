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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PriceHistoryRepository {

    private static final Logger log = LogManager.getLogger("PriceHistoryRepository");
    // will use Guava splitter rather than regex \s+
    static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();
    static final Splitter DATE_SPLITTER = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings();

    private final Path root;
    private final String prefix;
    private final String extension;
    private final Path cacheFile;

    PriceHistoryRepository(Path root, String prefix, String extension) {
        this.root = root;
        this.prefix = prefix;
        this.extension = extension;
        cacheFile = root.resolve(prefix + ".bin");
    }

    PriceHistory load() {
        try {
            PriceHistory priceHistory = useCachedFile() ? loadFromCache() : loadAndCache();
            log.info("file loaded");
            return priceHistory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    PriceHistory loadAndCache() throws IOException {
        PriceHistory priceHistory = loadImpl();
        saveToCache(priceHistory);
        return priceHistory;
    }

    private PriceHistory loadImpl() throws IOException {
        var files = list(root, prefix, extension);
        var parsedCsv = loadCsv(files);
        PriceHistory priceHistory = new PriceHistory(parsedCsv.size(), "date", "open", "high", "low", "close", "volume");
        int i = 0;
        LocalDateTime lastBar = null;
        for (IBCSV line : parsedCsv) {
            if (lastBar != null) {
                long diff = ChronoUnit.MINUTES.between(lastBar, line.date());
                if (diff <= 0) {
                    throw new RuntimeException("Current bar " + line.date() + " is before last bar " + lastBar);
                }
                if (diff > 1) {
                    log.info(String.format("Gap between %s and %s of %d minutes", lastBar, line.date(), diff));
                }
            }
            priceHistory.insert(i++, line.date(), line.open().doubleValue(), line.high().doubleValue(), line.low().doubleValue(), line.close().doubleValue(), line.volume());
            lastBar = line.date();
        }
        return priceHistory;
    }

    private boolean useCachedFile() throws IOException {
        if (!Files.exists(cacheFile)) {
            return false;
        }
        FileTime lastModifiedTime = Files.getLastModifiedTime(cacheFile);
        return lastModifiedTime.toInstant().isAfter(Instant.now().minusSeconds(60 * 60));
    }

    PriceHistory load(Path file) {
        try {
            var parsedCsv = loadCsv(List.of(file));
            PriceHistory priceHistory = new PriceHistory(parsedCsv.size(), "date", "open", "high", "low", "close", "volume");
            int i = 0;
            for (PriceHistoryRepository.IBCSV line : parsedCsv) {
                priceHistory.insert(i++, line.date(), line.open().doubleValue(), line.high().doubleValue(), line.low().doubleValue(), line.close().doubleValue(), line.volume());
            }
            return priceHistory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    PriceHistory loadFromCache() {
        log.info("Loading from cache file " + cacheFile);
        try (ObjectInputStream objectOutput = new ObjectInputStream(
                new GZIPInputStream(new FileInputStream(cacheFile.toFile())));) {
            return (PriceHistory) objectOutput.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    PriceHistory loadFromIBBars(List<Bar> bars) {
        PriceHistory priceHistory = new PriceHistory(bars.size(), "date", "open", "high", "low", "close", "volume");
        int i = 0;
        for (Bar bar : bars) {
            var dateParts = DATE_SPLITTER.splitToList(bar.time());
            priceHistory.insert(i++, LocalDateTime.of(LocalDate.parse(dateParts.get(0), DateTimeFormatter.BASIC_ISO_DATE), LocalTime.parse(dateParts.get(1))), bar.open(), bar.high(), bar.low(), bar.close(), (int) bar.volume().longValue());
        }
        return priceHistory;

    }

    void saveToCache(PriceHistory priceHistory) {
        log.info("Saving to cache file " + cacheFile);
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(
                new GZIPOutputStream(new FileOutputStream(cacheFile.toFile())))) {
            objectOutput.writeObject(priceHistory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    List<Path> list(Path root, String prefix, String extension) throws IOException {
        return Files.list(root)
                .filter(this::matches)
                .sorted()
                .toList();
    }

    boolean matches(Path path) {
        String fname = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return fname.startsWith(prefix) && fname.endsWith(extension);
    }

    List<IBCSV> loadCsv(List<Path> files) throws IOException {
        List<IBCSV> xs = new ArrayList<>();
        for (var file : files) {
            log.info("loading " + file);
            // skip header line which does not have an index
            xs.addAll(Files.lines(file)
                    .filter(e -> Character.isDigit(e.charAt(0)))
                    .map(IBCSV::parse)
                    .toList());
        }
        return xs;
    }

    record IBCSV(LocalDateTime date, BigDecimal open, BigDecimal high,
                 BigDecimal low, BigDecimal close, int volume, BigDecimal wap) {

        //0,20221204  23:00:00,4102.5,4107.25,4102.5,4105.0,34,4103.65,13
        static IBCSV parse(String s) {
            var xs = LINE_SPLITTER.splitToList(s);
            var ds = DATE_SPLITTER.splitToList(xs.get(1));
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
            // ignore bar count
        }
    }

}
