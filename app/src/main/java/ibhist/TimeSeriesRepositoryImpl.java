package ibhist;

import com.google.common.base.Suppliers;
import com.google.common.math.DoubleMath;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
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
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Supplier;

/**
 * Implementation of a repository for time series data.
 * see Time Series Collections in mongoDB help
 * The code uses the official MongoDB 5.6.x driver and types
 * Do not try to use Spring MongoDB driver or legacy 4.x driver code
 */
public class TimeSeriesRepositoryImpl implements TimeSeriesRepository {
    public static final String CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String DATABASE_NAME = "futures";
    public static final String M1_COLLECTION = "m1";
    public static final String TRADE_DATE_COLLECTION = "tradeDate";
    public static final String DAILY_COLLECTION = "daily";
    public static final String DAILY_RTH_COLLECTION = "dailyRth";
    private static final Logger log = LogManager.getLogger(TimeSeriesRepositoryImpl.class.getSimpleName());
    public static final ZoneId UTC = ZoneId.of("UTC");
    //    private final String connectionString;
//    private final String collectionName;
    private final Supplier<MongoClient> client;
    private final Supplier<MongoDatabase> db;

    public TimeSeriesRepositoryImpl() {
//        this.connectionString = Objects.requireNonNull(connectionString);
//        this.collectionName = Objects.requireNonNull(collectionName);
        client = Suppliers.memoize(this::createClient);
        db = Suppliers.memoize(() -> client.get().getDatabase(DATABASE_NAME));
    }

    MongoClient createClient() {
        var registry = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
        var codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), registry);
        var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(CONNECTION_STRING)).codecRegistry(codecRegistry).build();
        return MongoClients.create(settings);
    }

    private MongoCollection<Document> getCollection(String name) {
        return db.get().getCollection(name);
    }

    private <T> MongoCollection<T> getCollection(String name, Class<T> clz) {
        return db.get().getCollection(name, clz);
    }

    /**
     * drop existing collection if it exists and create a new time series collection
     */
    @Override
    public void newTimeSeriesCollection() {
        try {
            var c = getCollection(M1_COLLECTION);
            c.drop();
        } catch (IllegalArgumentException e) {
            log.error(e);
        }
        createM1TimeSeriesCollection("symbol", "timestamp");
    }

    @Override
    public void createM1TimeSeriesCollection(String metaName, String timestampName) {
        var options = new TimeSeriesOptions(timestampName)
                .granularity(TimeSeriesGranularity.MINUTES)
                .metaField(metaName);
        db.get().createCollection(M1_COLLECTION, new CreateCollectionOptions().timeSeriesOptions(options));
    }

    @Override
    public void createTradeDateIndexCollection(String name) {
        TimeSeriesOptions tsOptions = new TimeSeriesOptions("tradeDate")
                        .metaField("symbol")
                        .granularity(TimeSeriesGranularity.HOURS);

        db.get().createCollection(
                name,
                new CreateCollectionOptions().timeSeriesOptions(tsOptions)
        );

        // Enforce uniqueness of (symbol, tradeDate)
        db.get().getCollection(name).createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("symbol"),
                        Indexes.ascending("tradeDate")
                ),
                new IndexOptions()
        );
    }

    @Override
    public void newTradeDateIndexCollection(String name) {
        try {
            var c = getCollection(name);
            c.drop();
        } catch (IllegalArgumentException e) {
            log.error(e);
        }
        createTradeDateIndexCollection(name);
    }

    /**
     * Convert Date field to LocalDate time assuming the date is in UTC
     */
    private static @Nullable LocalDateTime asLocalDateTime(Document d, String fieldName) {
        Date dt = d.get(fieldName, Date.class);
        return dt == null ? null : asLocalDateTime(dt);
    }

    private static LocalDateTime asLocalDateTime(Date dt) {
        return LocalDateTime.ofInstant(dt.toInstant().truncatedTo(ChronoUnit.SECONDS), UTC);
    }

    private static Integer getNumberInserted(Optional<InsertManyResult> r) {
        return r.map(e -> e.getInsertedIds().size()).orElse(0);
    }

    @Override
    public void insert(PriceHistory history) {
        log.info("inserting into mongodb.futures.m1 {}", history);
        var imr = insert(getCollection(M1_COLLECTION), history);
        log.info("inserted documents {}", getNumberInserted(imr));
    }

    /**
     * Append to existing time series collection removing existing rows with different volume
     * @param history
     */
    @Override
    public void append(PriceHistory history) {
        try {
            log.info("inserting into mongodb.futures.m1 {}", history);
            var collection = getCollection(M1_COLLECTION);
            LocalDateTime last = removeExistingWithDifferentVol(collection, history);
            log.info("inserting rows after {}", last);
            var imr = insert(collection, history, last);
            log.info("inserted documents {}", getNumberInserted(imr));
        } catch (MongoTimeoutException e) {
            log.error(e);
        }
    }

    // last bar in time series may be incomplete
    private LocalDateTime removeExistingWithDifferentVol(MongoCollection<Document> collection, PriceHistory history) {
        LocalDateTime dt = null;
        while (dt == null) {
            var lastDoc = collection.find(Filters.eq("symbol", history.getSymbolLowerCase())).sort(Sorts.descending("timestamp")).first();
            if (lastDoc == null) {
                return null;
            }
            var timestamp = asLocalDateTime(lastDoc, "timestamp");
            var bar = history.bar(timestamp);
            if (bar.isPresent() && !DoubleMath.fuzzyEquals(lastDoc.getDouble("volume"), bar.get().volume(), 1e-6)) {
                log.info("deleting time series timestamp={}", timestamp);
                collection.deleteOne(Filters.and(Filters.eq("symbol", lastDoc.getString("symbol")), Filters.eq("timestamp", lastDoc.getDate("timestamp"))));
            } else {
                dt = timestamp;
            }
        }
        return dt;
    }

    @Override
    @Nullable
    public LocalDateTime findLastDate(MongoCollection<Document> collection, String symbol) {
        var item = collection.find(Filters.eq("symbol", symbol)).sort(Sorts.descending("timestamp")).projection(Projections.include("timestamp")).first();
        return asLocalDateTime(item, "timestamp");
    }


    @Override
    public PriceHistory loadSingleDay(String symbol, LocalDateTime start) {
        return makePriceHistory(loadBetween(symbol, start.atZone(UTC).toInstant(), start.plusDays(1).atZone(UTC).toInstant()));
    }

    /*
    select for a single trade date
    {
        symbol: {$eq: "esh3"},
        timestamp: {$gte: ISODate("2023-03-08T23:00:00Z"), $lt: ISODate("2023-03-09T23:00:00Z")}
    }
     */
    @Override
    public PriceHistory loadSingleDay(String symbol, LocalDate tradeDate) {
        var end = tradeDate.atTime(23, 0);
        var iEnd = end.atZone(UTC).toInstant();
        var iStart = end.minusDays(1).atZone(UTC).toInstant();
        return makePriceHistory(loadBetween(symbol, iStart, iEnd));
    }

    @Override
    public ArrayList<PriceBarM> loadBetween(String symbol, Instant iStart, Instant iEndExclusive) {
        log.info("loadBetween {} [{}, {})", symbol, iStart, iEndExclusive);
        var fSymbol = Filters.eq("symbol", symbol);
        var fStart = Filters.gte("timestamp", Date.from(iStart));
        var fEnd = Filters.lt("timestamp", Date.from(iEndExclusive));
        var filter = Filters.and(fSymbol, fStart, fEnd);

        return db.get().getCollection(M1_COLLECTION, PriceBarM.class).find(filter).into(new ArrayList<>());
    }

    PriceHistory makePriceHistory(List<PriceBarM> xs) {
        var history = new PriceHistory(xs.getFirst().symbol(), xs.size(), "date", "open", "high", "low", "close", "volume", "vwap");
        for (var x : xs) {
            history.add(x.timestamp(), x.open(), x.high(), x.low(), x.close(), x.volume(), x.vwap());
        }
        return history;
    }

    private Optional<InsertManyResult> insert(MongoCollection<Document> m1, PriceHistory history) {
        return insert(m1, history, null);
    }

    private Optional<InsertManyResult> insert(MongoCollection<Document> m1, PriceHistory history, LocalDateTime lastExisting) {
        LocalDateTime[] dates = history.dates;
        double[] opens = history.getColumn("open");
        double[] highs = history.getColumn("high");
        double[] lows = history.getColumn("low");
        double[] closes = history.getColumn("close");
        double[] volumes = history.getColumn("volume");
        double[] vwaps = history.getColumn("vwap");
        List<Document> rows = new ArrayList<>(dates.length);
        int start = lastExisting == null ? 0 : Math.max(0, history.find(lastExisting));
        for (int i = start; i < history.length(); i++) {
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
        return rows.isEmpty() ? Optional.empty() : Optional.of(m1.insertMany(rows));
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
        log.info("inserting threshold {}", rows.size());
        return m1.insertMany(rows);
    }

    /* equivalent aggregation pipeline
        $group: {
          _id: "$symbol",
          threshold: { $sum : 1 },
          start: { $first : "$timestamp" },
          end: { $last : "$timestamp" }
        }
     */
    protected List<SummaryMDB> queryM1SummaryImpl() {
        var collection = getCollection(M1_COLLECTION, SummaryMDB.class);
        // sum(returned field name, expression - typically "$field_name"
        // must sort before feeding into aggregation since the first/last operators are order dependent
        return collection.aggregate(List.of(
                Aggregates.sort(Sorts.ascending("symbol", "timestamp")),
                Aggregates.group(
                        // id field ie groupBy
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

    /**
     * return all Summary documents about the m1 data
     */
    @Override
    public List<Summary> queryM1Summary() {
        return queryM1SummaryImpl().stream()
                .map(s -> new Summary(s.symbol(), s.count(), s.high(), s.low(), asLocalDateTime(s.start()), asLocalDateTime(s.end())))
                .toList();
    }

    @Override
    public List<Summary> buildTradeDateIndex() {
        newTradeDateIndexCollection(TRADE_DATE_COLLECTION);
        var m = new HashMap<String, List<TradeDateIndexEntry>>();
        var summaries = queryM1Summary();
        for (var summary : summaries) {
            var entries = queryContiguousRegions(summary.symbol(), 30);
            m.put(summary.symbol(), entries);
        }
        log.info("loaded {} summaries", m.size());
        insertTradeDateIndexRows(m);
        return summaries;
    }

    public static final String CONTINUOUS_BARS_PIPELINE = """
                [
                    {"$match": {"symbol": "%s"}},
                    {"$sort": {"timestamp": 1}},
                    {"$setWindowFields": {
                        "sortBy": {"timestamp": 1},
                        "output": {
                            "prevTm": {"$shift": {"output": "$timestamp", "by": -1}},
                            "docNum": {"$documentNumber": {}}
                        }
                    }},
                    {"$set": {
                        "isNewRegion": {"$gt": [
                            {"$dateDiff": {"startDate": "$prevTm", "endDate": "$timestamp", "unit": "minute"}},
                            %d
                        ]}
                    }},
                    {"$setWindowFields": {
                        "sortBy": {"timestamp": 1},
                        "output": {"region": {"$sum": {"$cond": ["$isNewRegion", 1, 0]}, "window": {"documents": ["unbounded", "current"]}}}
                    }},
                    {"$group": {
                        "_id": "$region",
                        "start": {"$min": "$timestamp"},
                        "end": {"$max": "$timestamp"},
                        "open": {"$first": "$open"},
                        "high": {"$max": "$high"},
                        "low": {"$min": "$low"},
                        "close": {"$last": "$close"},
                        "volume": {"$sum": "$volume"},
                        "vwap": {"$last": "$vwap"}
                    }},
                    {"$sort": {"start": 1}},
                    {"$project": {
                        "_id": 0,
                        "start": 1, "end": 1, "open": 1, "high": 1,
                        "low": 1, "close": 1, "volume": 1, "vwap": 1
                    }}
                ]
            """;

    public List<TradeDateIndexEntry> queryContiguousRegions(String symbol, int gapMins) {

        String pipelineJson = CONTINUOUS_BARS_PIPELINE.formatted(symbol.toLowerCase(), gapMins);

        @SuppressWarnings("unchecked")
        var pipeline = (List<Document>) Document.parse("{ \"pipeline\": " + pipelineJson + " }").get("pipeline");

        var collection = getCollection(M1_COLLECTION, Document.class);
        var xs =  collection
                .aggregate(pipeline, ContiguousRegionMDB.class)
                .into(new ArrayList<>());
        // immediately convert to usable type dropping some fields
        return xs.stream()
                .map(e -> new TradeDateIndexEntry(asLocalDateTime(e.start()), asLocalDateTime(e.end()), (long)e.volume() ))
                .toList();
    }

    private static Date toDate(LocalDateTime ldt) {
        return ldt == null || ldt.equals(LocalDateTime.MIN)
                ? null
                : Date.from(ldt.toInstant(ZoneOffset.UTC));
    }

    void insertTradeDateIndexRows(Map<String, List<TradeDateIndexEntry>> data) {
        var collection = getCollection(TRADE_DATE_COLLECTION);
        InsertManyOptions options = new InsertManyOptions().ordered(false);

        for (var entry : data.entrySet()) {
            String symbol = entry.getKey();
            List<TradeDateIndexEntry> rows = entry.getValue();

            if (rows == null || rows.isEmpty()) {
                continue;
            }

            List<Document> docs = new ArrayList<>(rows.size());

            for (TradeDateIndexEntry e : rows) {
                Document doc = new Document()
                        .append("symbol", symbol)
                        .append("tradeDate", toDate(e.tradeDate().atStartOfDay()))
                        .append("start", toDate(e.start()))
                        .append("end", toDate(e.end()))
                        .append("volume", e.volume())
                        .append("duration", e.duration())
                        .append("rthStart", toDate(e.rthStart()))
                        .append("rthEnd", toDate(e.rthEnd()));

                docs.add(doc);
            }

            var r = collection.insertMany(docs, options);
            log.info("day index for symbol {} inserted {} ", symbol, r.getInsertedIds().size());
        }
    }

    public List<TradeDateIndexEntry> queryTradeDates(String symbol, long minVolume) {
        var collection = getCollection(TRADE_DATE_COLLECTION, TradeDateMDB.class);
        var filter = Filters.and(Filters.eq("symbol", symbol), Filters.gte("volume", minVolume));
        var results = collection.find(filter).sort(Sorts.ascending("tradeDate")).into(new ArrayList<>());
        return results.stream()
                .map(e -> new TradeDateIndexEntry(
                        asLocalDateTime(e.tradeDate()).toLocalDate(),
                        asLocalDateTime(e.start()),
                        asLocalDateTime(e.end()),
                        e.volume(),
                        e.duration(),
                        e.rthStart() == null ? LocalDateTime.MIN : asLocalDateTime(e.rthStart()),
                        e.rthEnd() == null ? LocalDateTime.MIN : asLocalDateTime(e.rthEnd())
                ))
                .toList();
    }

    public void close() {
        client.get().close();
    }

    public record SummaryMDB(@BsonId String symbol, int count, double high, double low, Date start, Date end) {}

    public record ContiguousRegionMDB(
            Date start,
            Date end,
            double open,
            double high,
            double low,
            double close,
            double volume,
            double vwap
    ) {}

    public record TradeDateMDB(
            Date tradeDate,
            Date start,
            Date end,
            long volume,
            long duration,
            Date rthStart,
            Date rthEnd
    ) {}
    /**
     * return list of first bar after a gap of 30 minutes. does not include first bar of collection
     */
    @Override
    public List<LocalDateTime> queryDayStartTimes(String symbol) {
        var window = WindowOutputFields.of(
                WindowOutputFields.shift("lastTm", "$timestamp", null, -1).toBsonField());
        Bson fields = Projections.fields(
                Projections.excludeId(),
                Projections.include("timestamp", "lastTm"),
                Projections.computed("gap", new Document("$dateDiff", new Document("startDate", "$lastTm").append("endDate", "$timestamp").append("unit", "minute"))));
        // compute difference in minutes {$dateDiff:{startDate: "$lastTm",endDate: "$timestamp",unit: "minute"} }
        try (var c = getCollection(M1_COLLECTION).aggregate(List.of(
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

    public record DayVolume(@BsonId Date date, int count, double volume) {
    }

    /**
     * find days with total volume >minVol
     * aggregate by calendar day
     */
    @Override
    public List<DayVolume> queryDaysWithVolume(String symbol, double minVol) {
        return getCollection(M1_COLLECTION, DayVolume.class).aggregate(List.of(
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


    // Add this record for daily bars (inside the class, near other records)
    public record DailyBarMDB(
            String symbol,
            Date tradeDate,
            double open,
            double high,
            double low,
            double close,
            int volume,
            double vwap
    ) {}

    // Create daily time series collection
    public void createDailyTimeSeriesCollection(String name) {
        TimeSeriesOptions tsOptions = new TimeSeriesOptions("tradeDate")
                .metaField("symbol")
                .granularity(TimeSeriesGranularity.HOURS);

        db.get().createCollection(
                name,
                new CreateCollectionOptions().timeSeriesOptions(tsOptions)
        );

        db.get().getCollection(name).createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("symbol"),
                        Indexes.ascending("tradeDate")
                ),
                new IndexOptions()
        );
    }

    /**
     * drops and creates an empty collection
     * @param name collection name
     */
    public void newDailyTimeSeriesCollection(String name) {
        try {
            getCollection(name).drop();
        } catch (IllegalArgumentException e) {
            log.error(e);
        }
        createDailyTimeSeriesCollection(name);
    }

    /**
     * Build daily OHLC bars from m1 data for a symbol using trade date boundaries.
     * @param symbol the futures contract symbol
     * @param useRth if true, use RTH times; otherwise use full session times
     * @return list of aggregated daily bars
     */
    public List<DailyBar> aggregateDailyBars(String symbol, boolean useRth) {
        List<TradeDateIndexEntry> tradeDates = queryTradeDates(symbol, 100_000);
        List<DailyBar> dailyBars = new ArrayList<>();

        for (TradeDateIndexEntry td : tradeDates) {
            LocalDateTime start = useRth ? td.rthStart() : td.start();
            LocalDateTime end = useRth ? td.rthEnd() : td.end();

            if (start == null || end == null ||
                    start.equals(LocalDateTime.MIN) || end.equals(LocalDateTime.MIN)) {
                log.warn("Skipping {} tradeDate={} - missing {} times",
                        symbol, td.tradeDate(), useRth ? "RTH" : "session");
                continue;
            }

            var bar = aggregateSingleDay(symbol, td.tradeDate(), start, end);
            if (bar != null) {
                dailyBars.add(bar);
            }
        }

        log.info("Aggregated {} daily bars for {} (rth={})", dailyBars.size(), symbol, useRth);
        return dailyBars;
    }

    /**
     * Aggregate m1 bars for a single trading day into one daily bar.
     */
    @Nullable
    private DailyBar aggregateSingleDay(String symbol, LocalDate tradeDate,
                                        LocalDateTime start, LocalDateTime end) {
        Instant iStart = start.atZone(UTC).toInstant();
        Instant iEnd = end.plusMinutes(1).atZone(UTC).toInstant(); // inclusive end

        var pipeline = List.of(
                Aggregates.match(Filters.and(
                        Filters.eq("symbol", symbol.toLowerCase()),
                        Filters.gte("timestamp", Date.from(iStart)),
                        Filters.lt("timestamp", Date.from(iEnd))
                )),
                Aggregates.sort(Sorts.ascending("timestamp")),
                Aggregates.group(
                        null,
                        Accumulators.first("open", "$open"),
                        Accumulators.max("high", "$high"),
                        Accumulators.min("low", "$low"),
                        Accumulators.last("close", "$close"),
                        Accumulators.sum("volume", "$volume"),
                        Accumulators.last("vwap", "$vwap"),
                        Accumulators.sum("count", 1)
                )
        );

        var result = getCollection(M1_COLLECTION).aggregate(pipeline).first();
        if (result == null || result.getInteger("count", 0) == 0) {
            log.warn("No m1 data for {} on {}", symbol, tradeDate);
            return null;
        }
        return new DailyBar(
                tradeDate,
                result.getDouble("open"),
                result.getDouble("high"),
                result.getDouble("low"),
                result.getDouble("close"),
                result.getDouble("volume").intValue(),
                result.getDouble("vwap")
        );
    }

    /**
     * Insert daily bars into the specified collection.
     */
    public void insertDailyBars(String collectionName, String symbol, List<DailyBar> bars) {
        if (bars.isEmpty()) {
            log.info("No bars to insert into {}", collectionName);
            return;
        }

        var collection = getCollection(collectionName);
        List<Document> docs = new ArrayList<>(bars.size());

        for (DailyBar bar : bars) {
            Document doc = new Document()
                    .append("symbol", symbol)
                    .append("tradeDate", toDate(bar.tradeDate().atStartOfDay()))
                    .append("open", bar.open())
                    .append("high", bar.high())
                    .append("low", bar.low())
                    .append("close", bar.close())
                    .append("volume", bar.volume())
                    .append("vwap", BigDecimal.valueOf(bar.vwap())
                            .setScale(3, RoundingMode.HALF_UP).doubleValue());
            docs.add(doc);
        }

        var result = collection.insertMany(docs, new InsertManyOptions().ordered(false));
        log.info("Inserted {} daily bars into {}", result.getInsertedIds().size(), collectionName);
    }

    /**
     * Build daily time series for a symbol (both full session and RTH).
     */
    public void buildDailyTimeSeries(String symbol) {
        log.info("Building daily time series for {}", symbol);
        insertDailyBars(DAILY_COLLECTION, symbol, aggregateDailyBars(symbol, false));
        insertDailyBars(DAILY_RTH_COLLECTION, symbol, aggregateDailyBars(symbol, true));
    }

    /**
     * Build daily time series for all symbols in the tradeDate index.
     */
    public void buildAllDailyTimeSeries() {
        newDailyTimeSeriesCollection(DAILY_COLLECTION);
        newDailyTimeSeriesCollection(DAILY_RTH_COLLECTION);

        var summaries = queryM1Summary();
        for (var summary : summaries) {
            buildDailyTimeSeries(summary.symbol());
        }
        log.info("Completed building daily time series for {} symbols", summaries.size());
    }

    /**
     * Query daily bars for a symbol.
     */
    public List<DailyBar> queryDailyBars(String symbol, boolean rth) {
        String collectionName = rth ? DAILY_RTH_COLLECTION : DAILY_COLLECTION;
        var collection = getCollection(collectionName, DailyBarMDB.class);

        var results = collection
                .find(Filters.eq("symbol", symbol.toLowerCase()))
                .sort(Sorts.ascending("tradeDate"))
                .into(new ArrayList<>());

        return results.stream()
                .map(d -> new DailyBar(
                        asLocalDateTime(d.tradeDate()).toLocalDate(),
                        d.open(),
                        d.high(),
                        d.low(),
                        d.close(),
                        d.volume(),
                        d.vwap()
                ))
                .toList();
    }

    /**
     * Query daily bars between dates (inclusive).
     */
    public List<DailyBar> queryDailyBars(String symbol, LocalDate startDt,
                                         LocalDate endDt, boolean rth) {
        String collectionName = rth ? DAILY_RTH_COLLECTION : DAILY_COLLECTION;
        var collection = getCollection(collectionName, DailyBarMDB.class);

        var filter = Filters.and(
                Filters.eq("symbol", symbol.toLowerCase()),
                Filters.gte("tradeDate", toDate(startDt.atStartOfDay())),
                Filters.lte("tradeDate", toDate(endDt.atStartOfDay()))
        );

        var results = collection
                .find(filter)
                .sort(Sorts.ascending("tradeDate"))
                .into(new ArrayList<>());

        return results.stream()
                .map(d -> new DailyBar(
                        asLocalDateTime(d.tradeDate()).toLocalDate(),
                        d.open(),
                        d.high(),
                        d.low(),
                        d.close(),
                        d.volume(),
                        d.vwap()
                ))
                .toList();
    }

}
