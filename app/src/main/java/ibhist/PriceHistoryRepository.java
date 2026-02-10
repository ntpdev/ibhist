package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
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
import java.util.*;
import java.util.regex.Pattern;
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
        return load(symbol, true, true);
    }

    Optional<PriceHistory> load(String symbol, boolean useCache, boolean addStandardColumns) {
        try {
            Path cacheFile = root.resolve(symbol + ".bin");
            var priceHistory = useCache && existsRecent(cacheFile, 60) ? loadFromCache(cacheFile) : loadAndCache(symbol, cacheFile);
            log.info("file loaded {}", priceHistory);
            if (priceHistory.isPresent() && addStandardColumns ) {
                //TODO where should this be done addStandardColumns();
            }
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
        var parsedCsv = loadCsv(findSymbolFiles(symbol));
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

    // primarily a testing function to load an arbitrary file
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

    /**
     * return ordered list of all futures symbols available
     */
    List<String> findAllSymbols() {
        return findAllCsvFiles()
                .stream()
                .map(e -> e.getFileName().toString().toLowerCase().substring(0, 4))
                .distinct()
                .toList();
    }

    /**
     * returns sorted list of paths to csv files for a symbol
     * @param symbol a futures symbol "esh6"
     * @return list of paths
     */
    List<Path> findSymbolFiles(String symbol) {
        return findAllCsvFiles().stream()
                .filter(e -> e.getFileName().toString().toLowerCase().startsWith(symbol))
                .toList();
    }

    /**
     * Returns a sorted list of CSV files containing futures contract data.
     * Files are sorted ascending by contract name, then by year and month.
     *
     * @return sorted list of paths to CSV files matching futures contract naming pattern
     */
    List<Path> findAllCsvFiles() {
        Pattern pattern = Pattern.compile("[a-z]{3}\\d");
        try (var paths = Files.find(root, 1, (path, attrs) -> {
            if (!attrs.isRegularFile()) {
                return false;
            }
            String filename = path.getFileName().toString().toLowerCase();
            if (!filename.endsWith(extension)) {
                return false;
            }
            return pattern.matcher(filename.substring(0, 4)).matches();
        })) {
            return paths
                    .sorted(Comparator.comparing(this::createContractKey))
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find CSV files in directory: " + root, e);
        }
    }

    /**
     * Creates a sort key for a futures contract file path.
     * Format: (contractName, year*100 + month)
     * Example: "esh5.csv" -> ("es", 503) where h=March=3, year=5
     */
    private record ContractKey(String contractName, int yearMonth) implements Comparable<ContractKey> {
        @Override
        public int compareTo(ContractKey other) {
            int nameComparison = this.contractName.compareTo(other.contractName);
            return nameComparison != 0 ? nameComparison : Integer.compare(this.yearMonth, other.yearMonth);
        }
    }

    private ContractKey createContractKey(Path path) {
        String filename = path.getFileName().toString().toLowerCase();
        String nameWithoutExt = filename.substring(0, filename.length() - 4);

        String contractName = nameWithoutExt.substring(0, 2);
        char monthChar = nameWithoutExt.charAt(2);
        int yearDigit = Character.getNumericValue(nameWithoutExt.charAt(3));

        int monthNumber = mapMonthCharToNumber(monthChar);
        int yearMonth = yearDigit * 100 + monthNumber;

        return new ContractKey(contractName, yearMonth);
    }

    /**
     * Maps futures contract month codes to month numbers.
     * Standard futures month codes:
     * F=Jan(1), G=Feb(2), H=Mar(3), J=Apr(4), K=May(5), M=Jun(6),
     * N=Jul(7), Q=Aug(8), U=Sep(9), V=Oct(10), X=Nov(11), Z=Dec(12)
     */
    private int mapMonthCharToNumber(char monthChar) {
        return switch (monthChar) {
            case 'f' -> 1;
            case 'g' -> 2;
            case 'h' -> 3;
            case 'j' -> 4;
            case 'k' -> 5;
            case 'm' -> 6;
            case 'n' -> 7;
            case 'q' -> 8;
            case 'u' -> 9;
            case 'v' -> 10;
            case 'x' -> 11;
            case 'z' -> 12;
            default -> throw new IllegalArgumentException("Invalid futures month code: " + monthChar);
        };
    }


    /**
     * load csv files. files are processed in order skipping duplicate timestamps
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

        //,Date,Open,High,Low,Close,Volume,WAP,BarCount
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
