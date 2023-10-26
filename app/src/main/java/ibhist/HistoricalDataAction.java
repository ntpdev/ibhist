package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class HistoricalDataAction extends ActionBase {
    //    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    private static final Splitter SPLITTER_WS = Splitter.on(CharMatcher.whitespace());
    private final Contract contract;
    private final Duration duration;

    private final List<Bar> bars = new ArrayList<>();

    public HistoricalDataAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract, Duration duration) {
        super(client, idGenerator, queue);
        this.contract = contract;
        this.duration = duration;
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
        client.reqHistoricalData(requestId, contract, "", duration.getCode(), "1 min", "TRADES", 0, 1, false, null);
    }

//    @Override
//    public void process() {
//        continuation.accept(this);
//    }

    @Override
    public void cancel() {
        client.cancelHistoricalData(requestId);
    }

    public void process(Bar bar) {
        bars.add(bar);
    }

    public PriceHistory getPriceHistory() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        return repository.loadFromIBBars(getSymbol(), bars);
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
        var dateParts = SPLITTER_WS.splitToList(bar.time());
        return String.format("%d,%s %s,%.2f,%.2f,%.2f,%.2f,%d,%.3f,%d", c, dateParts.get(0), dateParts.get(1), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue(), bar.wap().value().doubleValue(), bar.count());
    }
}
