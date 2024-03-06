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

import static ibhist.PriceHistoryRepository.WS_SPLITTER;

public class HistoricalDataAction extends ActionBase {
    //    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    private final Contract contract;
    private final Duration duration;
    private final boolean keepUpToDate;
    private int rtBarCount;
    private final List<Bar> bars = new ArrayList<>();
    private PriceHistory hist = null;
    private Bar lastBar = null;

    public HistoricalDataAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract, Duration duration, boolean keepUpToDate) {
        super(client, idGenerator, queue);
        this.contract = contract;
        this.duration = duration;
        this.keepUpToDate = keepUpToDate;
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
        client.reqHistoricalData(requestId, contract, "", duration.getCode(), "1 min", "TRADES", 0, 1, keepUpToDate, null);
    }

    @Override
    public void cancel() {
        client.cancelHistoricalData(requestId);
        process(); // base class will queue response
    }

    public void process(Bar bar) {
        bars.add(bar);
    }

    public void processEnd() {
        // if keepUpToDate=true updates will be streamed & processUpdate will be called
        if (!keepUpToDate) {
            process();
        }
    }

    public void processUpdate(Bar bar) {
        var history = getPriceHistory();
        var dt = history.getDates()[history.length() - 1];
        var barTime = parseTime(bar);
        if (dt.isEqual(barTime)) {
            history.replaceLast(barTime, bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue());
        } else {
            history.add(barTime, bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue());
            ++rtBarCount;
            if (rtBarCount > 9) {
                cancel();
            }
        }
        //TODO recalc rollhi
        var mx = history.getColumn("rollhi");
        var mn = history.getColumn("rolllo");
        double d = mx[mx.length - 1];
        double dmin = mn[mn.length - 1];
        log.info("rolling high/lo " + d + " " + dmin);
        bars.add(bar);
    }

    public PriceHistory getPriceHistory() {
        if (hist == null) {
            var repo = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
            hist = repo.loadFromIBBars(getSymbol(), bars);
            hist.rollingMax("high", 5, "rollhi");
            hist.rollingMin("low", 5, "rolllo");
        }
        return hist;
    }

    private String getSymbol() {
        return contract.localSymbol() == null ? contract.symbol() : contract.localSymbol();
    }

    public void save(LocalDate startDate) {
        try {
            String fname = "z" + getSymbol() + " " + startDate.format(DateTimeFormatter.BASIC_ISO_DATE) + ".csv";
            var p = Path.of("c:\\temp\\ultra\\", fname);
            log.info("saving file " + p);
            Files.writeString(p, getBarsAsCsv());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // mimic pandas output
    // ,Date,Open,High,Low,Close,Volume,WAP,BarCount
    // 13551,20230915  21:51:00,4501.75,4501.75,4501.0,4501.0,315,4501.3,72
    public String getBarsAsCsv() {
        var xs = new StringBuilder();
        xs.append(",Date,Open,High,Low,Close,Volume,WAP,BarCount");
        xs.append(System.lineSeparator());
        int c = 0;
        for (Bar bar : bars) {
            xs.append(barToCsv(c++, bar));
            xs.append(System.lineSeparator());
        }
        return xs.toString();
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
