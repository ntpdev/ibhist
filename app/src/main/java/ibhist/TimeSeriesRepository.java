package ibhist;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public interface TimeSeriesRepository {
    long RTH_START_OFFSET = 930;
    long RTH_END_OFFSET = 1319;

    void createM1TimeSeriesCollection(String collectionName, boolean dropExisting);

    void createDailyTimeSeriesCollection(String name, boolean dropExisting);

    void insert(PriceHistory history);

    int append(PriceHistory history);

    PriceHistory loadPriceHistory(String symbol, LocalDate startDate, LocalDate endDate, boolean rthOnly);

    List<PriceBarM> queryM1RowsBetween(String symbol, LocalDateTime start, LocalDateTime end);

    List<Summary> queryM1Summary();

    /**
     * rebuild the date collection based on m1 data
     * @return all summary info
     */
    List<Summary> buildTradeDateIndex();

    /**
     * rebuild the date documents for just the symbol based on m1 data
     * @param symbol symbol
     * @return a list containing the single summary rebuilt
     */
    List<Summary> buildTradeDateIndex(String symbol);

    List<TradeDateIndexEntry> queryContiguousRegions(String symbol, int gapMins);

    List<LocalDateTime> queryDayStartTimes(String symbol);

    List<TimeSeriesRepositoryImpl.DayVolume> queryDaysWithVolume(String symbol, double minVol);

    void buildAllDailyTimeSeries();

    void buildDailyTimeSeries(String symbol);

    List<DailyBar> queryDailyBars(String symbol, boolean rth);

    List<DailyBar> queryDailyBars(String symbol, LocalDate startInclusive, LocalDate endExclusive, boolean rth);

    record Summary(String symbol, int count, double high, double low, LocalDateTime start, LocalDateTime end) {
    }

    record TradeDateIndexEntry(
            LocalDate date,
            LocalDateTime start,
            LocalDateTime end,
            long volume,
            long duration, // duration in minutes
            LocalDateTime rthStart, // optional MIN_DATETIME indicates partial day with no rthStart
            LocalDateTime rthEnd // optional but will be present if rthStart is present
    ) {

        public TradeDateIndexEntry(LocalDateTime start, LocalDateTime end, long volume) {
            this(
                    end.toLocalDate(),
                    start,
                    end,
                    volume,
                    0,
                    LocalDateTime.MIN,
                    LocalDateTime.MIN
            );
        }

        public TradeDateIndexEntry {
            if (start == null || end == null) {
                throw new IllegalArgumentException("start and end must not be null");
            }

            // validate end.date() - start.date() == 1
            if (!end.toLocalDate().equals(start.toLocalDate().plusDays(1))) {
                throw new IllegalArgumentException("end date must be exactly one day after start date");
            }

            duration = java.time.Duration.between(start, end).toMinutes() + 1;

            rthStart = LocalDateTime.MIN;
            rthEnd = LocalDateTime.MIN;

            if (duration > RTH_START_OFFSET) {
                LocalDateTime computedRthStart = start.plusMinutes(RTH_START_OFFSET);
                LocalDateTime computedRthEnd = start.plusMinutes(RTH_END_OFFSET);

                rthStart = computedRthStart.isAfter(end) ? end : computedRthStart;
                rthEnd = computedRthEnd.isAfter(end) ? end : computedRthEnd;
            }

            date = end.toLocalDate();
        }
    }

    record DailyBar(
            LocalDate date,
            double open,
            double high,
            double low,
            double close,
            int volume,
            double vwap
    ) {
    }
}
