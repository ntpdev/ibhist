package ibhist;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Optional;

/**
 * PriceHistoryRepository contains methods for loading and saving PriceHistory to csv files
 */
public interface PriceHistoryRepository {
    Optional<PriceHistory> load(String symbol);

    /**
     * loads all data files in repository for a symbol returns a single history
     */
    Optional<PriceHistory> load(String symbol, boolean useCache, boolean addStandardColumns);

    PriceHistory load(String symbol, Path file);

    /**
     * saves content to a file with name "symbol date.csv" "ESH6 20260130.csv"
     * @param symbol
     * @param startDate should be first trade date in file
     * @param csvContent
     * @return
     */
    Path saveCsv(String symbol, LocalDate startDate, String csvContent);
}