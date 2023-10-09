package ibhist;

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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class TimeSeriesRepository {

    private static final Logger log = LogManager.getLogger("TimeSeriesRepository");
    private final String connectionString;

    public TimeSeriesRepository(String connectionString) {
        this.connectionString = Objects.requireNonNull(connectionString);
    }

    public void test() {
        try (MongoClient mongoClient = createClient()) {
            MongoDatabase db = mongoClient.getDatabase("futures");
//            createTimeSeriesCollection(db, "m1", "symbol", "timestamp");
            var collection = db.getCollection("m1");
            log.info(summary(collection));
            var m1 = db.getCollection("m1", PriceBarM.class);
            var fSymbol = Filters.eq("symbol", "esz3");
            var xs = m1.find(fSymbol).into(new ArrayList<>());
            log.info(xs.size());
            log.info(xs.get(xs.size()-1));
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

    MongoClient createClient() {
        var registry = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
        var codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), registry);
        var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString)).codecRegistry(codecRegistry).build();
        return MongoClients.create(settings);
    }

    /**
     * Convert Date field to LocalDate time assuming the date is in UTC
     */
    private static @Nullable LocalDateTime getLocalDateTime(Document d, String fieldName) {
        Date dt = d.get(fieldName, Date.class);
        return dt == null ? null : LocalDateTime.ofInstant(dt.toInstant().truncatedTo(ChronoUnit.SECONDS), ZoneId.of("UTC"));
    }

    void createTimeSeriesCollection(MongoDatabase db, String collectionName, String metaName, String timestampName) {
        TimeSeriesOptions options = new TimeSeriesOptions(timestampName)
                .granularity(TimeSeriesGranularity.SECONDS)
                .metaField(metaName);
        db.createCollection(collectionName, new CreateCollectionOptions().timeSeriesOptions(options));
    }

    void insert(MongoCollection<Document> collection) {
        var instant = Instant.now();
        var d1 = new Document("timestamp", Date.from(instant)).append("symbol", "sym1").append("open", 1235.56d);
        var d2 = new Document("timestamp", Date.from(instant)).append("symbol", "sym2").append("open", 1237.6d);
        InsertManyResult insertManyResult = collection.insertMany(List.of(d1, d2));
    }

    public static void main(String[] args) {
        new TimeSeriesRepository("mongodb://localhost:27017").test();
    }

    public void insert(PriceHistory history) {
        log.info("inserting into " + connectionString + " " + history.length());
        try (MongoClient mongoClient = createClient()) {
            MongoDatabase db = mongoClient.getDatabase("futures");
//            createTimeSeriesCollection(db, "m1", "symbol", "timestamp");
//            MongoCollection<Document> m1 = db.getCollection("m1");
//            insert(m1, history);
            var collection = db.getCollection("m1", PriceBarM.class);
            insertEx(collection, history);
        }
    }

    private InsertManyResult insert(MongoCollection<Document> m1, PriceHistory history) {
        LocalDateTime[] dates = history.dates;
        double[] opens = history.getColumn("open");
        double[] highs = history.getColumn("high");
        double[] lows = history.getColumn("low");
        double[] closes = history.getColumn("close");
        double[] volumes = history.getColumn("volume");
        double[] vwaps = history.getColumn("vwap");
        List<Document> rows = new ArrayList<>(dates.length);
        for (int i = 0; i < dates.length; i++) {
            var d = new Document("symbol", "esz3")
                    .append("timestamp", dates[i])
                    .append("open", opens[i])
                    .append("high", highs[i])
                    .append("low", lows[i])
                    .append("close", closes[i])
                    .append("volume", volumes[i])
                    .append("vwap", BigDecimal.valueOf(vwaps[i]).setScale(3, RoundingMode.HALF_UP).doubleValue());
            rows.add(d);
        }
        log.info("inserting rows " + rows.size());
        return m1.insertMany(rows);
    }

    // inserts using POJO
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
            var d = new PriceBarM("esz3", dates[i], opens[i], highs[i], lows[i], closes[i], volumes[i], BigDecimal.valueOf(vwaps[i]).setScale(3, RoundingMode.HALF_UP).doubleValue());
            rows.add(d);
        }
        log.info("inserting rows " + rows.size());
        return m1.insertMany(rows);
    }

        private Summary summary(MongoCollection<Document> collection) {
        // sum(returned field name, expression - typically "$field_name"
        var xs = collection.aggregate(List.of(
                Aggregates.match(Filters.eq("symbol", "esz3")),
                Aggregates.group("$symbol", List.of(
                        Accumulators.sum("count", 1),
                        Accumulators.max("high", "$high"),
                        Accumulators.min("low", "$low"),
                        Accumulators.first("earliest", "$timestamp"),
                        Accumulators.last("latest", "$timestamp")
                ))
        )).into(new ArrayList<>());
        return Summary.newSummary(xs.get(0));
    }

    record Summary(String symbol, int rows, LocalDateTime start, LocalDateTime end, double high, double low) {
        static Summary newSummary(Document d) {
            return new Summary(
                    d.getString("_id"),
                    d.getInteger("count"),
                    getLocalDateTime(d, "earliest"),
                    getLocalDateTime(d, "latest"),
                    d.getDouble("high"),
                    d.getDouble("low") );
        }
    }
}
