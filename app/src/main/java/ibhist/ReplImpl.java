package ibhist;

import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static ibhist.StringUtils.print;

public class ReplImpl implements Repl {
    private static final Logger log = LogManager.getLogger(ReplImpl.class.getSimpleName());
    private final IBConnector connector;
    private final Supplier<PriceHistoryRepository> repository = Suppliers.memoize(() -> new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv"));
    private PriceHistory history = null;

    @Inject
    public ReplImpl(IBConnector connector) {
        this.connector = connector;
    }

    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            log.error(e);
        }
        print("[blue]bye[/]");
    }

    void runImpl() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        print("[yellow]enter command\nx| load sym | info n | ib | p n | minmax n=15\nx - exit\nload zesu4\ninfo 1\nib - load single day from ib\nrt - real time bars\np n - print first or last n bars or bars at time hh:mm[/]\n");
        String line;
        MonitorManager monitorManager = new MonitorManager();
        while ((line = reader.readLine()) != null) {
            var input = StringUtils.split(line);
            if (!input.isEmpty()) {
                print("[yellow]" + line + "[/]");
                String cmd = input.get(0).toLowerCase();
                String arg1 = input.size() > 1 ? input.get(1) : null;
                if (cmd.equals("x")) {
                    break;
                } else if (cmd.equals("load") && arg1 != null) {
                    load(arg1);
                } else if (cmd.equals("info") && history != null) {
                    info(parseInt(arg1, -1));
                } else if (cmd.equals("ib")) {
                    requestHistoricData();
                } else if (cmd.equals("rt")) {
                    processRt(input, monitorManager);
                } else if (cmd.equals("minmax") && history != null) {
                    printMinMax(parseInt(arg1, 15));
                } else if (cmd.equals("p") && arg1 != null && history != null) {
                    // param 1 = offset, -offset, hh:mm
                    if (arg1.contains(":")) {
                        PriceHistory.Index index = history.index();
                        var entries = index.entries();
                        tryParseTime(arg1, entries.getLast().tradeDate()).ifPresent(barTime -> {
                            int i = history.find(barTime);
                            if (i > 0) {
                                printHistory(i - 9, i + 10);
                            }
                        });
                    } else {
                        tryParseInt(arg1).ifPresent(this::printHistory);
                    }
                } else {
                    // process "add monitor > 5924.25"
                    monitorManager.processCommand(input);
                }
            }
        }
        reader.close();
    }

    private void requestHistoricData() {
        connector.connect();
        var action = connector.getHistoricalData("ES", IBConnectorImpl.CONTRACT_MONTH, Duration.DAY_5, false);
        history = action.asPriceHistory();
        connector.disconnect();
    }

    // commands rt / rt cancel
    private boolean processRt(List<String> input, MonitorManager monitorManager) {
        if (input.getFirst().equalsIgnoreCase("rt")) {
            if (input.size() == 1) {
                connector.connect();
                connector.requestRealTimeBars("ES", IBConnectorImpl.CONTRACT_MONTH, monitorManager);
                print("[blue]requesting realtime bars[/]");
            } else {
                connector.cancelRealtime();
                connector.disconnect();
                print("[blue]client disconnect[/]");
            }
            return true;
        }
        return false;
    }

    private static int parseInt(String s, int defaultValue) {
        if (s == null || s.isBlank()) {
            return defaultValue;
        }
        return tryParseInt(s).orElse(defaultValue);
    }

    private static OptionalInt tryParseInt(String s) {
        try {
            return OptionalInt.of(Integer.parseInt(s));
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
    }

    private static Optional<LocalDateTime> tryParseTime(String s, LocalDate dt) {
        try {
            return Optional.of(dt.atTime(LocalTime.parse(s, DateTimeFormatter.ofPattern("H:mm"))));
        } catch (DateTimeParseException e) {
            log.error(e);
            return Optional.empty();
        }
    }

    private void printHistory(int n) {
        print(history.asTextTable(n));
    }

    private void printHistory(int start, int end) {
        print(history.asTextTable(start, end));
    }

    private void printMinMax(int n) {
        var hi = history.localMax("high", n, "local_high");
        var lo = history.localMin("low", n, "local_low");
        StringBuilder sb = new StringBuilder();
        sb.append("\n[yellow]local high / low %d[/]\n".formatted(n));
        for (int i = history.length() - 240; i < history.length(); ++i) {
            if (hi.values[i] > 0) {
                sb.append(String.format("[green]%s %.2f hi ▲[/]\n", history.getDates()[i].toLocalTime(), hi.values[i]));
            }
            if (lo.values[i] > 0) {
                sb.append(String.format("[red]%s %.2f lo ▼[/]\n", history.getDates()[i].toLocalTime(), lo.values[i]));
            }
        }
        sb.append("[yellow]---[/]");
        print(sb.toString());
    }

    void load(String s) {
        var optH = repository.get().load(s, false);
        if (optH.isEmpty()) return;
        history = optH.get();
        var vwap = history.vwap("vwap");
        var strat = history.strat("strat");
        StringBuilder sb = new StringBuilder();
        sb.append("[yellow]");
        for (var entry : history.index().entries()) {
            sb.append(entry).append(System.lineSeparator());
        }
        sb.append("[/]");
        print(sb);
    }


    void info(int n) {
        PriceHistory.Index index = history.index();
        List<PriceHistory.IndexEntry> entries = index.entries();
        n = (n + entries.size()) % entries.size();
        PriceHistory.IndexEntry entry = entries.get(n);
        var map = index.makeMessages(entry, n >= 1 ? entries.get(n - 1) : null);
        var sb = new StringBuilder();
        sb.append("\n[yellow]---\ntrade date ").append(entry.tradeDate().toString()).append("[/]\n\n");
        for (String s : map.reversed().values()) {
            sb.append(s).append(System.lineSeparator());
        }
        print(sb);
        //TODO: figure this out and add to history.print to can see volume spikes
//        if (entry.rthStart() > 0) {
//            var rollingStandardize = history.rolling_standardize("volume", 30, "volstd");
//            var values = PriceHistory.standardize(history.getColumn("volume"), entry.rthStart(), entry.end() + 1);
//            for (int i = entry.rthStart(); i < entry.end() + 1; i++) {
//                if (rollingStandardize.values[i] > 2 || values[i - entry.rthStart()] > 2) {
//                    print(history.bar(i).toString() + " " + values[i - entry.rthStart()] + " " + rollingStandardize.values[i], ANSI_CYAN);
//                }
//            }
//        }
    }
}
