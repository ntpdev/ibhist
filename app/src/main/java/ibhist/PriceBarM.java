package ibhist;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;

public record PriceBarM(
    @BsonId ObjectId id,
    String symbol,
    LocalDateTime timestamp,
    double open,
    double high,
    double low,
    double close,
    double volume,
    double vwap,
    double ema) {}

