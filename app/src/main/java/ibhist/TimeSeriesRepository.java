package ibhist;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public interface TimeSeriesRepository {
    long RTH_START_OFFSET = 930;
    long RTH_END_OFFSET = 1319;

    void createM1TimeSeriesCollection(String collectionName, boolean dropExisting);

    void createMinVolTimeSeriesCollection(String name, boolean dropExisting);

    void createDailyTimeSeriesCollection(String name, boolean dropExisting);

    void insertM1(PriceHistory history);

    void insertMinVol(PriceHistory history);

    int append(PriceHistory history);

    /**
     *
     * updates the minvol collection with new minvol data based on the last day in history.
     * Used to keep an existing minvol collection upto date
     * @param history
     * @return
     */
    LocalDateTime updateMinvolForLastDay(PriceHistory history);

    /**
     * selectively rebuild the minvol collection for the interval of the price history
     * @param history
     */
    LocalDateTime rebuildMinVol(PriceHistory history);

    /**
     * create a PriceHistory covering inclusive trade dates.
     */
    PriceHistory loadPriceHistory(String symbol, LocalDate startDate, LocalDate endDate, boolean rthOnly);

    /**
     * create a PriceHistory covering the slice of trading days from index to index+(length-1).
     */
    PriceHistory loadPriceHistory(String symbol, int index, int length);

    List<PriceBarM> queryM1RowsBetween(String symbol, LocalDateTime start, LocalDateTime end);

    List<Summary> queryM1Summary();

    /**
     * rebuild the trade_date_index collection based on m1 data
     *
     * @return list of symbols affected
     */
    List<String> buildTradeDateIndex();

    /**
     * rebuild the trade_date_index for a single symbol based on m1 data
     *
     * @param symbol symbol
     * @return a list of symbols affected
     */
    List<String> buildTradeDateIndex(String symbol);

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
            long bars, // number of m1 bars
            LocalDateTime rthStart, // MIN_DATETIME indicates partial day with no rthStart
            LocalDateTime rthEnd // MIN_DATETIME present if rthStart is present
    ) {

        public TradeDateIndexEntry(LocalDateTime start, LocalDateTime end, long volume, long bars) {
            this(
                    end.toLocalDate(),
                    start,
                    end,
                    volume,
                    bars,
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

            if (bars != java.time.Duration.between(start, end).toMinutes() + 1) {
                throw new IllegalArgumentException("number of bars does not match time range");
            }

            rthStart = LocalDateTime.MIN;
            rthEnd = LocalDateTime.MIN;

            if (bars > RTH_START_OFFSET) {
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
