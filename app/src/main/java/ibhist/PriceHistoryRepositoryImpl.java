package ibhist;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static ibhist.StringUtils.WS_SPLITTER;

public class PriceHistoryRepositoryImpl implements PriceHistoryRepository {

    private static final Logger log = LogManager.getLogger(PriceHistoryRepositoryImpl.class.getSimpleName());
    static final Splitter COMMA_SPLITTER = Splitter.on(",").trimResults();
    static final String[] IB_COLUMNS = "date open high low close volume wap".split(" ");
    private static final Pattern FUTURES_TICKER = Pattern.compile("^([A-Z]+)[FGHJKMNQUVXZ]\\d$", Pattern.CASE_INSENSITIVE);

    private final Path root;
    private final String extension;

    @Inject
    public PriceHistoryRepositoryImpl() {
        this(Paths.get(System.getProperty("user.home"), "Documents", "data"), ".csv");
    }

    public PriceHistoryRepositoryImpl(Path root, String extension) {
        this.root = root;
        this.extension = extension;
    }

    public Optional<PriceHistory> load(String symbol) {
        return load(symbol, true, true);
    }

    @Override
    public Optional<PriceHistory> load(String symbol, boolean useCache, boolean addStandardColumns) {
        try {
            Path cacheFile = root.resolve(symbol + ".bin");
            var priceHistory = useCache && existsRecent(cacheFile, 60) ? loadFromCache(cacheFile) : loadAndCache(symbol, cacheFile);
            log.info("file loaded {}", priceHistory);
            if (addStandardColumns) {
                priceHistory.ifPresent(PriceHistory::addStandardColumns);
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
        var priceHistory = loadImpl(symbol, symbol);
        priceHistory.ifPresent(history -> saveToCache(cacheFile, history));
        return priceHistory;
    }

    private Optional<PriceHistory> loadImpl(String symbol, String prefix) throws IOException {
        var parsedCsv = loadCsv(findAllDataFiles(prefix));
        if (!parsedCsv.isEmpty()) {
            PriceHistory priceHistory = new PriceHistory(symbol, parsedCsv.size(), IB_COLUMNS);
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
                priceHistory.insert(i++, line.date(), line.open().doubleValue(), line.high().doubleValue(), line.low().doubleValue(), line.close().doubleValue(), line.volume(), line.wap().doubleValue());
                lastBar = line.date();
            }
            return Optional.of(priceHistory);
        }
        return Optional.empty();
    }

    // primarily a testing function to load an arbitrary file
    @Override
    public PriceHistory load(String symbol, Path file) {
        try {
            var parsedCsv = loadCsv(List.of(file));
            PriceHistory priceHistory = new PriceHistory(symbol, parsedCsv.size(), IB_COLUMNS);
            int i = 0;
            for (var line : parsedCsv) {
                priceHistory.insert(i++, line.date(), line.open().doubleValue(), line.high().doubleValue(), line.low().doubleValue(), line.close().doubleValue(), line.volume(), line.wap().doubleValue());
            }
            return priceHistory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Path saveCsv(String symbol, LocalDate startDate, String csvContent) {
        try {
            String fname = "z%s %s%s".formatted(symbol,startDate.format(DateTimeFormatter.BASIC_ISO_DATE), extension);
            Path p = root.resolve(fname);
            log.info("saving file {}", p);
            return Files.writeString(p, csvContent);
        } catch (IOException e) {
            throw new RuntimeException("error saving historical data", e);
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
     * return ordered (lowercase) list of all futures symbols available, this will include zes zmes files
     */
    List<String> findAllFuturesSymbols() {
        return findAllDataFiles().stream()
                .map(e -> WS_SPLITTER.splitToList(e.getFileName().toString().toLowerCase()).getFirst())
                .filter(e ->  e.length() >= 4 && FUTURES_TICKER.matcher(e).matches() )
                .distinct()
                .toList();
    }

    /**
     * returns sorted stream of paths to csv files with a prefix
     *
     * @param prefix a futures prefix "esh6"
     * @return list of paths
     */
    List<Path> findAllDataFiles(String prefix) {
        return findAllDataFiles().stream()
                .filter(e -> e.getFileName().toString().toLowerCase().startsWith(prefix))
                .toList();
    }

    /**
     * Returns a sorted stream of CSV files containing stock or futures data.
     * Files must match the pattern: {@code <ticker> <YYYYMMDD>.csv}
     * e.g. "TICK-NYSE 20251230.csv", "ESH5 20250130.csv"
     * Sorting by filename gives natural grouping by ticker, then ascending date.
     * @return sorted list of paths to CSV files matching the naming pattern
     */
    List<Path> findAllDataFiles() {
        Pattern pattern = Pattern.compile("^[\\w-]+ \\d{8}\\.csv$", Pattern.CASE_INSENSITIVE);
        try (var paths = Files.find(root, 1, (path, attrs) -> {
            if (!attrs.isRegularFile()) {
                return false;
            }
            return pattern.matcher(path.getFileName().toString()).matches();
        })) {
            return paths.sorted(Comparator.comparing(this::createSortKey)).toList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find CSV files in directory: " + root, e);
        }
    }

    private String createSortKey(Path path) {
        String filename = path.getFileName().toString();
        String ticker = filename.substring(0, filename.indexOf(' '));
        String date = filename.substring(filename.length() - 12, filename.length() - 4);
        var m = FUTURES_TICKER.matcher(ticker);
        return (m.matches() ? m.group(1) : ticker) + date;
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
     *
     * @param files
     * @return
     * @throws IOException
     */
    List<IBCSV> loadCsv(List<Path> files) throws IOException {
        List<IBCSV> xs = new ArrayList<>();
        for (var file : files) {
            LocalDateTime hw = xs.isEmpty() ? LocalDateTime.MIN : xs.getLast().date();
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
