package ibhist;

import com.google.common.base.Suppliers;
import com.google.common.math.DoubleMath;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.InsertManyResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.conversions.Bson;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class TimeSeriesRepository {
    private static final Logger log = LogManager.getLogger("TimeSeriesRepository");
    public static final ZoneId UTC = ZoneId.of("UTC");
    private final String connectionString;
    private final String collectionName;
    private final Supplier<MongoClient> client;
    private final Supplier<MongoDatabase> db;

    public TimeSeriesRepository(String connectionString, String dbName, String collectionName) {
        this.connectionString = Objects.requireNonNull(connectionString);
        this.collectionName = Objects.requireNonNull(collectionName);
        client = Suppliers.memoize(this::createClient);
        db = Suppliers.memoize(() -> client.get().getDatabase(dbName));
    }

    MongoClient createClient() {
        var registry = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
        var codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), registry);
        var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString)).codecRegistry(codecRegistry).build();
        return MongoClients.create(settings);
    }

    private MongoCollection<Document> getCollection() {
        return db.get().getCollection(collectionName);
    }

    private <T> MongoCollection<T> getCollection(Class<T> clz) {
        return db.get().getCollection(collectionName, clz);
    }

    /**
     * drop existing collection if it exists and create a new time series collection
     */
    void newTimeSeriesCollection() {
        try {
            var c = getCollection();
            c.drop();
        } catch (IllegalArgumentException e) {
            log.error(e);
        }
        createTimeSeriesCollection("symbol", "timestamp");
    }

    void createTimeSeriesCollection(String metaName, String timestampName) {
        TimeSeriesOptions options = new TimeSeriesOptions(timestampName)
                .granularity(TimeSeriesGranularity.MINUTES)
                .metaField(metaName);
        db.get().createCollection(collectionName, new CreateCollectionOptions().timeSeriesOptions(options));
    }

    /**
     * Convert Date field to LocalDate time assuming the date is in UTC
     */
    private static @Nullable LocalDateTime asLocalDateTime(Document d, String fieldName) {
        Date dt = d.get(fieldName, Date.class);
        return dt == null ? null : LocalDateTime.ofInstant(dt.toInstant().truncatedTo(ChronoUnit.SECONDS), ZoneId.of("UTC"));
    }

    public void insert(PriceHistory history) {
        log.info("inserting into mongodb.futures.m1 " + history);
        var r = insert(getCollection(), history);
        log.info("inserted documents " + r.getInsertedIds().size());
    }

    public void append(PriceHistory history) {
        log.info("inserting into mongodb.futures.m1 " + history);
        var collection = getCollection();
        LocalDateTime last = removeExistingWithDifferentVol(collection, history);
        log.info("inserting rows after " + last);
        insert(collection, history, last);
    }

    // last bar in time series may be incomplete
    private LocalDateTime removeExistingWithDifferentVol(MongoCollection<Document> collection, PriceHistory history) {
        LocalDateTime dt = null;
        while (dt == null) {
            var lastDoc = collection.find(Filters.eq("symbol", history.getSymbolLowerCase())).sort(Sorts.descending("timestamp")).first();
            var timestamp = asLocalDateTime(lastDoc, "timestamp");
            var bar = history.bar(timestamp);
            if (bar.isPresent() && !DoubleMath.fuzzyEquals(lastDoc.getDouble("volume"), bar.get().volume(), 1e-6)) {
                log.info("deleting time series timestamp=" + timestamp);
                collection.deleteOne(Filters.and(Filters.eq("symbol", lastDoc.getString("symbol")), Filters.eq("timestamp", lastDoc.getDate("timestamp"))));
            } else {
                dt = timestamp;
            }
        }
        return dt;
    }

    @Nullable
    LocalDateTime findLastDate(MongoCollection<Document> collection, String symbol) {
        var item = collection.find(Filters.eq("symbol", symbol)).sort(Sorts.descending("timestamp")).projection(Projections.include("timestamp")).first();
        return asLocalDateTime(item, "timestamp");
    }


    PriceHistory loadSingleDay(String symbol, LocalDateTime start) {
        return makePriceHistory(loadBetween(symbol, start.atZone(UTC).toInstant(), start.plusDays(1).atZone(UTC).toInstant()));
    }

    PriceHistory loadSingleDay(String symbol, LocalDate tradeDate) {
        var end = tradeDate.atTime(23, 0);
        var iEnd = end.atZone(UTC).toInstant();
        var iStart = end.minusDays(1).atZone(UTC).toInstant();
        return makePriceHistory(loadBetween(symbol, iStart, iEnd));
    }

    PriceHistory makePriceHistory(List<PriceBarM> xs) {
        var history = new PriceHistory(xs.get(0).symbol(), xs.size(), "date", "open", "high", "low", "close", "volume", "vwap");
        int i = 0;
        for (var x : xs) {
            history.insert(i++, x.timestamp(), x.open(), x.high(), x.low(), x.close(), x.volume(), x.vwap());
        }
        return history;
    }

    /*
    select for a single trade date
    {
        symbol: {$eq: "esh3"},
        timestamp: {$gte: ISODate("2023-03-08T23:00:00Z"), $lt: ISODate("2023-03-09T23:00:00Z")}
    }
     */
    private ArrayList<PriceBarM> loadBetween(String symbol, Instant iStart, Instant iEndExclusive) {
        log.info("loadBetween " + symbol + " [" + iStart + ", " + iEndExclusive + ")");
        var fSymbol = Filters.eq("symbol", symbol);
        var fStart = Filters.gte("timestamp", Date.from(iStart));
        var fEnd = Filters.lt("timestamp", Date.from(iEndExclusive));
        var filter = Filters.and(fSymbol, fStart, fEnd);

        return db.get().getCollection(collectionName, PriceBarM.class).find(filter).into(new ArrayList<>());
    }

    private InsertManyResult insert(MongoCollection<Document> m1, PriceHistory history) {
        return insert(m1, history, null);
    }

    private InsertManyResult insert(MongoCollection<Document> m1, PriceHistory history, LocalDateTime lastExisting) {
        LocalDateTime[] dates = history.dates;
        double[] opens = history.getColumn("open");
        double[] highs = history.getColumn("high");
        double[] lows = history.getColumn("low");
        double[] closes = history.getColumn("close");
        double[] volumes = history.getColumn("volume");
        double[] vwaps = history.getColumn("vwap");
        List<Document> rows = new ArrayList<>(dates.length);
        int start = lastExisting == null ? 0 : Math.max(0, history.find(lastExisting));
        for (int i = start; i < dates.length; i++) {
            if (lastExisting == null || dates[i].isAfter(lastExisting)) {
                var d = new Document("symbol", history.getSymbolLowerCase())
                        .append("timestamp", dates[i])
                        .append("open", opens[i])
                        .append("high", highs[i])
                        .append("low", lows[i])
                        .append("close", closes[i])
                        .append("volume", volumes[i])
                        .append("vwap", BigDecimal.valueOf(vwaps[i]).setScale(3, RoundingMode.HALF_UP).doubleValue());
                rows.add(d);
            }
        }
        return m1.insertMany(rows);
    }

    // inserts using record
    private InsertManyResult insertEx(MongoCollection<PriceBarM> m1, PriceHistory history) {
        LocalDateTime[] dates = history.dates;
        double[] opens = history.getColumn("open");
        double[] highs = history.getColumn("high");
        double[] lows = history.getColumn("low");
        double[] closes = history.getColumn("close");
        double[] volumes = history.getColumn("volume");
        double[] vwaps = history.getColumn("vwap");
        List<PriceBarM> rows = new ArrayList<>(dates.length);
        for (int i = 0; i < dates.length; i++) {
            var d = new PriceBarM(null, history.getSymbolLowerCase(), dates[i], opens[i], highs[i], lows[i], closes[i], volumes[i], BigDecimal.valueOf(vwaps[i]).setScale(3, RoundingMode.HALF_UP).doubleValue());
            rows.add(d);
        }
        log.info("inserting count " + rows.size());
        return m1.insertMany(rows);
    }

    /* equivalent aggregation pipeline
        $group: {
          _id: "$symbol",
          count: { $sum : 1 },
          start: { $first : "$timestamp" },
          end: { $last : "$timestamp" }
        }
     */
    List<Summary> summary() {
        var collection = getCollection(Summary.class);
        // sum(returned field name, expression - typically "$field_name"
        return collection.aggregate(List.of(
                Aggregates.group(
                        // id field
                        "$symbol",
                        // aggregates
                        Accumulators.sum("count", 1),
                        Accumulators.max("high", "$high"),
                        Accumulators.min("low", "$low"),
                        Accumulators.first("start", "$timestamp"),
                        Accumulators.last("end", "$timestamp")),
                Aggregates.sort(Sorts.ascending("start"))
        )).into(new ArrayList<>());
    }

    public void close() {
        client.get().close();
    }

    public record Summary(@BsonId String symbol, int count, double high, double low, Date start, Date end) {}

    /**
     * return list of first bar after a gap of 30 minutes. does not include first bar of collection
     */
    List<LocalDateTime> queryDayStartTimes(String symbol) {
        var window = WindowOutputFields.of(
                WindowOutputFields.shift("lastTm", "$timestamp", null, -1).toBsonField());
        Bson fields = Projections.fields(
                Projections.excludeId(),
                Projections.include("timestamp", "lastTm"),
                Projections.computed("gap", new Document("$dateDiff", new Document("startDate", "$lastTm").append("endDate", "$timestamp").append("unit", "minute"))));
        // compute difference in minutes {$dateDiff:{startDate: "$lastTm",endDate: "$timestamp",unit: "minute"} }
        try (var c = getCollection().aggregate(List.of(
                        Aggregates.match(Filters.eq("symbol", symbol)),
                        Aggregates.setWindowFields(null, Sorts.ascending("timestamp"), window),
                        Aggregates.project(fields),
                        Aggregates.match(Filters.gte("gap", 30))))
                .cursor()) {
            List<LocalDateTime> xs = new ArrayList<>();
            while (c.hasNext()) {
                Document d = c.next();
                xs.add(asLocalDateTime(d, "timestamp"));
            }
            log.info(xs);
            return xs;
        }
    }

    public record DayVolume(@BsonId Date date, int count, double volume) {}

    /**
     * find days with total volume >minVol
     */
    List<DayVolume> queryDaysWithVolume(String symbol, double minVol) {
        return getCollection(DayVolume.class).aggregate(List.of(
                Aggregates.match((Filters.eq("symbol", symbol))),
                Aggregates.group(
                        // id - groupBy fields {$dateTrunc: {date: "$timestamp", unit: "day"} }
                        new Document("$dateTrunc", new Document("date", "$timestamp").append("unit", "day")),
                        // aggregration fields in group
                        Accumulators.sum("count", 1),
                        Accumulators.sum("volume", "$volume")),
                Aggregates.match(Filters.gte("volume", minVol)),
                Aggregates.sort(Sorts.ascending("_id"))
        )).into(new ArrayList<>());
    }
}
