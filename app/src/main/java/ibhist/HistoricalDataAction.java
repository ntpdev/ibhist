package ibhist;

import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static ibhist.StringUtils.WS_SPLITTER;

/**
 * Handles 3 TWS callbacks
 * historicalData, historicalDataEnd, historicalDataUpdate
 */
public class HistoricalDataAction extends ActionBase {
    //    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    private final Contract contract;
    private final Duration duration;
    private final boolean keepUpToDate;
    private final LocalDateTime updateUntil;
    private final List<Bar> bars = new ArrayList<>();
    private PriceHistory hist = null;
    private int currentBarCount = 99;

    public HistoricalDataAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract, Duration duration, boolean keepUpToDate) {
        super(client, idGenerator, queue);
        this.contract = contract;
        this.duration = duration;
        this.keepUpToDate = keepUpToDate;
        this.updateUntil = keepUpToDate ? LocalDateTime.now().plusMinutes(5) : null;
    }

    public Contract getContract() {
        return contract;
    }

    public List<Bar> getBars() {
        return bars;
    }

    @Override
    public void makeRequest() {
//        var d = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).minusMinutes(1);
//        var upTo = d.format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")); 20231001 22:00:00
        log.info("reqHistoricalData reqId = {} symbol = {} duration = {}", requestId, contract.localSymbol(), duration.getCode());
        client.reqHistoricalData(requestId, contract, "", duration.getCode(), "1 min", "TRADES", 0, 1, keepUpToDate, null);
    }

    @Override
    public void cancel() {
        client.cancelHistoricalData(requestId);
        complete(); // base class will queue response
    }

    public void process(Bar bar) {
        bars.add(bar);
    }

    public void processEnd() {
        // marks end of historic data. if keepUpToDate=true future updates will be streamed
        // via processUpdate so this will not be the end
        if (!keepUpToDate) {
            complete();
        }
    }

    public void processUpdate(Bar bar) {
//        log.info(barToCsv(currentBarCount, bar));
        if (bar.volume().longValue() < 0) {
            log.info("ignoring bar with neg vol " + bar.time());
            return;
        }
        // replace or insert bar
        if (bars.getLast().time().equals(bar.time())) {
            bars.removeLast();
            bars.add(bar);
            // show history when last bar of current minute received
            if (--currentBarCount == 0) {
//                int n = bars.size();
//                bars.subList(n - 120, n);
                var hist = PriceHistory.createFromIBBars(getSymbol(), bars);
                hist.vwap("vwap");
                hist.strat("strat");
                StringUtils.print(hist.toString());
                StringUtils.print(hist.asTextTable(-15));
            }
        } else {
            bars.add(bar);
            currentBarCount = 11;
        }

        if (keepUpToDate && currentBarCount == 0 && parseTime(bar).isAfter(updateUntil)) {
            cancel();
        }
    }
//        log.info(barToCsv(n, bars.get(n)));
//        log.info(barToCsv(n+1, bars.get(n+1)));
//        var history = asPriceHistory();
//        var dt = history.getDates()[history.length() - 1];
//        var barTime = parseTime(bar);
//        if (dt.isEqual(barTime)) {
//            history.replace(-1, barTime, bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue());
//        } else {
//            //TODO calc vwap. saving to mongo requires vwap calc
//            history.add(barTime, bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue());
//            history.recalc(10);
//            log.info(history.info());
//            ++rtBarCount;
//            if (rtBarCount > 9) {
//                cancel();
//            }
//        }
//
//        //
//        int live = history.length() - 1;
//        int sz = 5;
//        var bar2 = history.aggregrate(live - 2 * sz, live - sz - 1);
//        var bar1 = history.aggregrate(live - sz, live - 1);
//        if (bar1.low() < bar2.low()) { // 2 down
//            String s = "2 down - long entry trigger over %f low %f".formatted(bar1.high(), bar1.low());
//            if (!s.equals(lastMsg)) {
//                log.info(s);
//                lastMsg = s;
//            }
//            if (bar.close() > bar1.high())
//                log.info("triggered " + history.bar(history.length() - 1));
//        }
//        bars.add(bar);

    public PriceHistory asPriceHistory() {
        if (hist == null) {
            hist = PriceHistory.createFromIBBars(getSymbol(), bars);
            hist.rollingMax("high", 5, "rollhi");
            hist.rollingMin("low", 5, "rolllo");
            hist.vwap("vwap");
            hist.strat("strat");
            hist.hilo("high", "hc");
            hist.hilo("low", "lc");
        }
        return hist;
    }

    private String getSymbol() {
        return contract.localSymbol() == null ? contract.symbol() : contract.localSymbol();
    }

    /**
     * mimic pandas output
     * ,Date,Open,High,Low,Close,Volume,WAP,BarCount
     * 13551,20230915  21:51:00,4501.75,4501.75,4501.0,4501.0,315,4501.3,72
     *
     * @return csv string
     */
    public String barsAsCsv() {
        var xs = new StringBuilder();
        xs.append(",Date,Open,High,Low,Close,Volume,WAP,BarCount").append(System.lineSeparator());
        int c = 0;
        for (Bar bar : bars) {
            xs.append(barToCsv(c++, bar)).append(System.lineSeparator());
        }
        return xs.toString();
    }

    public Path save(LocalDate startDate) {
        //TODO: could move to PriceHistoryRepository and have that injected but then PHR would need
        // to be a guice class and Action as well with a custom factory
        try {
            String fname = "z%s %s.csv".formatted(getSymbol(), startDate.format(DateTimeFormatter.BASIC_ISO_DATE));
            var p = Path.of("c:\\temp\\ultra\\", fname);
            log.info("saving file " + p);
            Files.writeString(p, barsAsCsv());
            return p;
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException("error saving historical data", e);
        }
    }

    private String barToCsv(int c, Bar bar) {
        // time = 20230919 23:00:00 Europe/London
        var timeParts = WS_SPLITTER.splitToList(bar.time());
        return String.format("%d,%s %s,%.2f,%.2f,%.2f,%.2f,%d,%.3f,%d", c, timeParts.get(0), timeParts.get(1), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue(), bar.wap().value().doubleValue(), bar.count());
    }

    private LocalDateTime parseTime(Bar bar) {
        var timeParts = WS_SPLITTER.splitToList(bar.time());
        return LocalDateTime.of(LocalDate.parse(timeParts.get(0), DateTimeFormatter.BASIC_ISO_DATE),
                LocalTime.parse(timeParts.get(1)));
    }
}
