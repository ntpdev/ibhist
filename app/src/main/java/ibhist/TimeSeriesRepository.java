package ibhist;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import org.jetbrains.annotations.Nullable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public interface TimeSeriesRepository {
    long RTH_START_MINUTES = 930;
    long RTH_END_MINUTES = 1319;

    void newTimeSeriesCollection();

    void createM1TimeSeriesCollection(String metaName, String timestampName);

    void newTradeDateIndexCollection(String name);

    void createTradeDateIndexCollection(String name);

    void insert(PriceHistory history);

    void append(PriceHistory history);

    @Nullable
    LocalDateTime findLastDate(MongoCollection<Document> collection, String symbol);

    PriceHistory loadSingleDay(String symbol, LocalDateTime start);

    PriceHistory loadSingleDay(String symbol, LocalDate tradeDate);


    ArrayList<PriceBarM> loadBetween(String symbol, Instant iStart, Instant iEndExclusive);

    /* equivalent aggregation pipeline
            $group: {
              _id: "$symbol",
              threshold: { $sum : 1 },
              start: { $first : "$timestamp" },
              end: { $last : "$timestamp" }
            }
         */
    List<Summary> queryM1Summary();

    List<Summary> buildTradeDateIndex();

    List<TradeDateIndexEntry> queryContiguousRegions(String symbol, int gapMins);

    List<LocalDateTime> queryDayStartTimes(String symbol);

    List<TimeSeriesRepositoryImpl.DayVolume> queryDaysWithVolume(String symbol, double minVol);

    void buildAllDailyTimeSeries();
    void buildDailyTimeSeries(String symbol);
    List<DailyBar> queryDailyBars(String symbol, boolean rth);
    List<DailyBar> queryDailyBars(String symbol, LocalDate startInclusive, LocalDate endExclusive, boolean rth);

    record Summary(String symbol, int count, double high, double low, LocalDateTime start, LocalDateTime end) {}

    record TradeDateIndexEntry(
            LocalDate tradeDate,
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

            if (duration > RTH_START_MINUTES) {
                LocalDateTime computedRthStart = start.plusMinutes(RTH_START_MINUTES);
                LocalDateTime computedRthEnd = start.plusMinutes(RTH_END_MINUTES);

                rthStart = computedRthStart.isAfter(end) ? end : computedRthStart;
                rthEnd = computedRthEnd.isAfter(end) ? end : computedRthEnd;
            }

            tradeDate = end.toLocalDate();
        }
    }

    record DailyBar(
            LocalDate tradeDate,
            double open,
            double high,
            double low,
            double close,
            int volume,
            double vwap
    ) {}
}
