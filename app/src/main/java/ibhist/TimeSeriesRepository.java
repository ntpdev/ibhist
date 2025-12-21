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
    void newTimeSeriesCollection();

    void createTimeSeriesCollection(String metaName, String timestampName);

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
    List<TimeSeriesRepositoryImpl.Summary> summary();

    List<LocalDateTime> queryDayStartTimes(String symbol);

    List<TimeSeriesRepositoryImpl.DayVolume> queryDaysWithVolume(String symbol, double minVol);
}
