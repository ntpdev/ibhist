package ibhist;

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

public class TimeSeriesRepository {
    private static final Logger log = LogManager.getLogger("TimeSeriesRepository");

    public static final ZoneId UTC = ZoneId.of("UTC");
    private final String connectionString;

    public TimeSeriesRepository(String connectionString) {
        this.connectionString = Objects.requireNonNull(connectionString);
    }

    public static void main(String[] args) {
        new TimeSeriesRepository("mongodb://localhost:27017").test2();
    }

    public void test() {
        try (MongoClient mongoClient = createClient()) {
            MongoDatabase db = mongoClient.getDatabase("futures");
//            createTimeSeriesCollection(db, "m1", "symbol", "timestamp");
            var collection = db.getCollection("m1");
            log.info(summary(collection));
//            var m1 = db.getCollection("m1", PriceBarM.class);
//            var fSymbol = Filters.eq("symbol", "esz3");
//            var xs = m1.find(fSymbol).into(new ArrayList<>());
//            log.info(xs.size());
//            log.info(xs.get(xs.size()-1));
//            db.createCollection("m1");
//            log.info(m1.find().sort(Sorts.descending("timestamp")).first());
            //Filters.gte("Open", 4500.0d)
//            Instant inst = Instant.parse("2023-10-09T12:00:00Z");
//            var fTimestamp = Filters.gte("timestamp", Date.from(inst));
//            var filter = Filters.and(fSymbol, fTimestamp);
//            AggregateIterable<Document> aggregate = m1.aggregate(List.of(Aggregates.group("$symbol", Accumulators.max("op", "$open"))));
//            try (var cursor = aggregate.cursor()) {
//                while (cursor.hasNext()) {
//                    var d = cursor.next();
//                    log.info(d.toJson());
//                }
//            }
//
//            try (var cursor = m1.find(filter).sort(Sorts.descending("timestamp")).limit(10).cursor()) {
//                while (cursor.hasNext()) {
//                    Document x = cursor.next();
//                    log.info(x.toJson());
//                    log.info(x.get("timestamp").getClass().getCanonicalName() + " " + getTimestamp(x));
//                }
//            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    void test2() {
        var xs = load("esh3", LocalDate.of(2023, 3, 9));
        log.info(xs);
        log.info(xs.rthBars());
    }

    MongoClient createClient() {
        var registry = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
        var codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), registry);
        var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString)).codecRegistry(codecRegistry).build();
        return MongoClients.create(settings);
    }

    /**
     * drop existing collection if it exists and create a new time series collection
     */
    MongoCollection<Document> newTimeSeriesCollection(MongoDatabase db, String collectionName) {
        try {
            var c = db.getCollection(collectionName);
            return c;
//            c.drop();
        } catch (IllegalArgumentException e) {
            log.error(e);
        }
        createTimeSeriesCollection(db, collectionName, "symbol", "timestamp");
        return db.getCollection(collectionName);
    }

    void createTimeSeriesCollection(MongoDatabase db, String collectionName, String metaName, String timestampName) {
        TimeSeriesOptions options = new TimeSeriesOptions(timestampName)
                .granularity(TimeSeriesGranularity.SECONDS)
                .metaField(metaName);
        db.createCollection(collectionName, new CreateCollectionOptions().timeSeriesOptions(options));
    }

    /**
     * Convert Date field to LocalDate time assuming the date is in UTC
     */
    private static @Nullable LocalDateTime getLocalDateTime(Document d, String fieldName) {
        Date dt = d.get(fieldName, Date.class);
        return dt == null ? null : LocalDateTime.ofInstant(dt.toInstant().truncatedTo(ChronoUnit.SECONDS), ZoneId.of("UTC"));
    }

    void insert(MongoCollection<Document> collection) {
        var instant = Instant.now();
        var d1 = new Document("timestamp", Date.from(instant)).append("symbol", "sym1").append("open", 1235.56d);
        var d2 = new Document("timestamp", Date.from(instant)).append("symbol", "sym2").append("open", 1237.6d);
        InsertManyResult insertManyResult = collection.insertMany(List.of(d1, d2));
    }

    public void insert(PriceHistory history) {
        log.info("inserting into " + connectionString + " " + history);
        try (MongoClient mongoClient = createClient()) {
            MongoDatabase db = mongoClient.getDatabase("futures");
//            createTimeSeriesCollection(db, "m1", "symbol", "timestamp");
//            MongoCollection<Document> m1 = db.getCollection("m1");
//            insert(m1, history);
            var collection = newTimeSeriesCollection(db, "m1");
            var r = insert(collection, history);
            log.info("inserted documents " + r.getInsertedIds().size());
        }
    }

    public void append(PriceHistory history) {
        log.info("inserting into " + connectionString + " futures/m1 " + history);
        try (MongoClient mongoClient = createClient()) {
            MongoDatabase db = mongoClient.getDatabase("futures");
//            createTimeSeriesCollection(db, "m1", "symbol", "timestamp");
//            MongoCollection<Document> m1 = db.getCollection("m1");
//            insert(m1, history);
            var collection = db.getCollection("m1");
            LocalDateTime last = removeExistingWithDifferentVol(collection, history);
            log.info("inserting rows after " + last);
            insert(collection, history, last);
        }
    }

    // last bar in time series may be incomplete
    private LocalDateTime removeExistingWithDifferentVol(MongoCollection<Document> collection, PriceHistory history) {
        LocalDateTime dt = null;
        while (dt == null) {
            var lastDoc = collection.find(Filters.eq("symbol", history.getSymbolLowerCase())).sort(Sorts.descending("timestamp")).first();
            var timestamp = getLocalDateTime(lastDoc, "timestamp");
            var bar = history.bar(timestamp);
            if (!DoubleMath.fuzzyEquals(lastDoc.getDouble("volume"), bar.volume(), 1e-6)) {
                log.info("deleting time series timestamp=" + timestamp);
                collection.deleteOne(Filters.and(Filters.eq("symbol", lastDoc.getString("symbol")), Filters.eq("timestamp", lastDoc.getDate("timestamp"))));
            } else {
                dt = timestamp;
            }
        }
        return dt;
    }

    @Nullable LocalDateTime findLastDate(MongoCollection<Document> collection, String symbol) {
        var item = collection.find(Filters.eq("symbol", symbol)).sort(Sorts.descending("timestamp")).projection(Projections.include("timestamp")).first();
        return getLocalDateTime(item, "timestamp");
    }

    PriceHistory load(String symbol, LocalDate tradeDate) {
        var xs = loadImpl(symbol, tradeDate);
        var history = new PriceHistory(symbol, xs.size(), "date", "open", "high", "low", "close", "volume", "vwap");
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
    List<PriceBarM> loadImpl(String symbol, LocalDate tradeDate) {
        try (MongoClient mongoClient = createClient()) {
            MongoDatabase db = mongoClient.getDatabase("futures");
            var collection = db.getCollection("m1", PriceBarM.class);
            var end = tradeDate.atTime(23, 0);
            var iEnd = end.atZone(UTC).toInstant();
            var iStart = end.minusDays(1).atZone(UTC).toInstant();

            var fSymbol = Filters.eq("symbol", symbol);
            var fStart = Filters.gte("timestamp", Date.from(iStart));
            var fEnd = Filters.lt("timestamp", Date.from(iEnd));
            var filter = Filters.and(fSymbol, fStart, fEnd);
            return collection.find(filter).into(new ArrayList<>());
        }
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
            if (lastExisting != null && dates[i].isAfter(lastExisting)) {
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
        {
          _id: "$symbol",
          count: { $sum : 1 },
          start: { $first : "$timestamp" },
          end: { $last : "$timestamp" }
        }
     */
    private List<Summary> summary(MongoCollection<Document> collection) {
        // sum(returned field name, expression - typically "$field_name"
        var xs = collection.aggregate(List.of(
//                Aggregates.match(Filters.eq("symbol", "esz3")),
                Aggregates.group("$symbol", List.of(
                        Accumulators.sum("count", 1),
                        Accumulators.max("high", "$high"),
                        Accumulators.min("low", "$low"),
                        Accumulators.first("start", "$timestamp"),
                        Accumulators.last("end", "$timestamp")
                ))
        )).into(new ArrayList<>());
        return xs.stream().map(Summary::newSummary).toList();
    }

    record Summary(String symbol, int count, LocalDateTime start, LocalDateTime end, double high, double low) {
        static Summary newSummary(Document d) {
            return new Summary(
                    d.getString("_id"),
                    d.getInteger("count"),
                    getLocalDateTime(d, "start"),
                    getLocalDateTime(d, "end"),
                    d.getDouble("high"),
                    d.getDouble("low"));
        }
    }
}
