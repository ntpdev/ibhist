package ibhist;

import com.ib.client.*;

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
    private final LocalDate endDate;
    private final Duration duration;
    private final boolean keepUpToDate;
    private final LocalDateTime updateUntil;
    private final List<Bar> bars = new ArrayList<>();
    private final MonitorManager monitorManager;
    private final List<PriceMonitor> monitors = new ArrayList<>();
    private PriceHistory hist = null;
    private int currentBarCount = 99;
    private boolean init = false;

    public HistoricalDataAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract, LocalDate endDate, Duration duration, boolean keepUpToDate, MonitorManager monitorManager) {
        super(client, idGenerator, queue);
        this.contract = contract;
        this.endDate = endDate;
        this.duration = duration;
        this.keepUpToDate = keepUpToDate;
        this.updateUntil = keepUpToDate ? LocalDateTime.now().plusMinutes(15) : null;
        this.monitorManager = monitorManager;
    }

    public Contract getContract() {
        return contract;
    }

    public List<Bar> getBars() {
        return bars;
    }

    @Override
    public void makeRequest() {
        String upTo = "";
        if (endDate != null) {
            var d = endDate.atTime(22, 59);
            upTo = d.format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"));
            upTo = upTo + " Europe/London"; // ask for local time. the option is to use exchange tz from contractDetails
        }
//        var d = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).minusMinutes(1);
//        var upTo = d.format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")); 20231001 22:00:00
        log.info("reqHistoricalData reqId = {} symbol = {} bars = {}", requestId, contract.localSymbol(), duration.getCode());
        // add end explicit end date to get up to that point
//        client.reqHistoricalData(requestId, contract, "20250718 23:00:00 Europe/London", bars.getCode(), "1 min", "TRADES", 0, 1, keepUpToDate, null);
        client.reqHistoricalData(requestId, contract, upTo, duration.getCode(), "1 min", "TRADES", 0, 1, keepUpToDate, null);
    }

    @Override
    public void cancel() {
        client.cancelHistoricalData(requestId);
        complete(); // base class will queue response
    }

    public void onHistoricalData(Bar bar) {
        bars.add(bar);
    }

    public void onHistoricalDataEnd() {
        // marks end of historic data.
        // if keepUpToDate=true future updates will be streamed via processUpdate
        // so this will not be the last call
        if (!keepUpToDate) {
            complete();
        }
    }

    public void onHistoricalDataUpdate(Bar bar) {
//        log.info(barToCsv(currentBarCount, bar));
        if (bar.volume().longValue() < 0) {
            log.info("ignoring bar with neg vol " + bar.time());
            return;
        }
        if (!init) {
            init();
            init = true;
        }
        // replace or insert bar
        if (bars.getLast().time().equals(bar.time())) {
            bars.removeLast();
            bars.add(bar);
            for (var m : monitors) {
                m.test(bar.close());
            }
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

    private void init() {
        for (var data : monitorManager) {
            monitors.add(new PriceMonitor(data, this::eventHandler));
        }
    }

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

    public String getSymbol() {
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

    private void eventHandler(PriceEvent event) {
        log.info(event.toString());
        switch (event.state()) {
            case ChangeState.entry -> {
                var ord = new OrderDetails(Types.Action.BUY, OrderType.LMT, 6049.50, 1, Types.TimeInForce.DAY);
                var group = ord.createOrderGroup(32, 32);
//                new OrderBuilder(null, null).placeOrders(group);
            }

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
