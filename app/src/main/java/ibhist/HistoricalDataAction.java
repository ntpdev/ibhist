package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class HistoricalDataAction extends ActionBase {
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    private static final Splitter SPLIT_WS = Splitter.on(CharMatcher.whitespace());
    private static final Logger log = LogManager.getLogger("HistoricalDataAction");
    private final Consumer<HistoricalDataAction> continuation;
    private final Contract contract;

    private final List<Bar> bars = new ArrayList<>();

    public HistoricalDataAction(EClientSocket client, AtomicInteger idGenerator, Contract contract, Consumer<HistoricalDataAction> continuation) {
        super(client, idGenerator);
        this.continuation = continuation;
        this.contract = contract;
    }

    public Contract getContract() {
        return contract;
    }

    public List<Bar> getBars() {
        return bars;
    }

    @Override
    public void request() {
//        var d = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).minusMinutes(1);
//        var upTo = d.format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")); 20231001 22:00:00
        client.reqHistoricalData(requestId, contract, "", "10 D", "1 min", "TRADES", 0, 1, false, null);
    }

    @Override
    public void process() {
        continuation.accept(this);
    }

    @Override
    public void cancel() {
        client.cancelHistoricalData(requestId);
    }

    public void process(Bar bar) {
        bars.add(bar);
    }

    // mimic pandas output
    // ,Date,Open,High,Low,Close,Volume,WAP,BarCount
    // 13551,20230915  21:51:00,4501.75,4501.75,4501.0,4501.0,315,4501.3,72
    public List<String> getBarsAsCsv() {
        var xs = new ArrayList<String>();
        xs.add(",Date,Open,High,Low,Close,Volume,WAP,BarCount");
        int c = 0;
        for (Bar bar : bars) {
            xs.add(barToCsv(c++, bar));
        }
        return xs;
    }

    public PriceHistory getPriceHistory() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), "esu3", ".csv");
        return repository.loadFromIBBars(bars);
    }

    public void save() {
        try {
            var p = Path.of("c:\\temp\\ultra\\", contract.localSymbol());
            log.info("saving file " + p);
            Files.write(p, getBarsAsCsv());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String barToCsv(int c, Bar bar) {
        // time = 20230919 23:00:00 Europe/London
        var dateParts = SPLIT_WS.splitToList(bar.time());
        return String.format("%d,%s %s,%.2f,%.2f,%.2f,%.2f,%d,%.3f,%d", c, dateParts.get(0), dateParts.get(1), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume().longValue(), bar.wap().value().doubleValue(), bar.count());
    }
}
