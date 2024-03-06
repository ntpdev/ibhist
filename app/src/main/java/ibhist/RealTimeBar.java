package ibhist;

import java.time.LocalDateTime;

public record RealTimeBar(LocalDateTime dt, double open, double high, double low, double close, double volume, double wap) {}

