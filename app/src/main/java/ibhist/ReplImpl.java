package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
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
import java.util.List;
import java.util.function.Supplier;

public class ReplImpl implements Repl {
    private static final Logger log = LogManager.getLogger(ReplImpl.class.getSimpleName());
    static final Splitter WS_SPLITTER = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings();
    static final String ANSI_RESET = "\u001B[0m";
    static final String ANSI_GREEN = "\u001B[32m";
    static final String ANSI_YELLOW = "\u001B[33m";
    static final String ANSI_CYAN = "\u001B[36m";
    static final String ANSI_RED = "\u001B[31m";
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
        } catch (IOException e) {
            log.error(e);
        }
    }

    void runImpl() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        StringColourise.print("[yellow]enter command x|load sym|info n|ib|p n\nx - exit\nload zesu4\ninfo 1\nib - load single day from ib\nrt - real time bars\np n - print first or last n bars[/]");
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> input = WS_SPLITTER.splitToList(line);
            if (!input.isEmpty()) {
                StringColourise.print("[yellow]" + line + "[/]");
                String cmd = input.get(0).toLowerCase();
                String arg1 = input.size() > 1 ? input.get(1) : null;
                if (cmd.equals("x")) {
                    break;
                } else if (cmd.equals("load") && arg1 != null) {
                    load(arg1);
                } else if (cmd.equals("info") && arg1 != null && history != null) {
                    info(Integer.parseInt(arg1));
                } else if (cmd.equals("ib")) {
                    connector.process(IBConnector.ConnectorAction.ES_DAY);
                } else if (cmd.equals("rt")) {
                    connector.process(IBConnector.ConnectorAction.REALTIME);
                } else if (cmd.equals("minmax") && arg1 != null && history != null) {
                    printMinMax(Integer.parseInt(arg1));
                } else if (cmd.equals("p") && arg1 != null) {
                    // param 1 = offset, -offset, hh:mm
                    if (arg1.contains(":")) {
                        PriceHistory.Index index = history.index();
                        var entries = index.entries();
                        var barTime = makeDateTime(arg1, entries.getLast().tradeDate());
                        int i = history.find(barTime);
                        if (i > 0) {
                            printHistory(i - 5, i + 6);
                        }
                    } else {
                        printHistory(Integer.parseInt(arg1));
                    }
                }
            }
        }
        reader.close();
    }

    private static LocalDateTime makeDateTime(String s, LocalDate dt) {
        return dt.atTime(LocalTime.parse(s, DateTimeFormatter.ISO_TIME));
    }

    private void printHistory(int n) {
        System.out.println(history.debugPrint(n));
    }

    private void printHistory(int start, int end) {
        System.out.println(history.debugPrint(start, end));
    }

    private void printMinMax(int n) {
        var hi = history.localMax("high", n, "local_high");
        var lo = history.localMin("low", n, "local_low");
        StringBuilder sb = new StringBuilder();
        sb.append("local high lows\n");
        for (int i = history.length() - 120; i < history.length(); ++i) {
            if (hi.values[i] > 0) {
                sb.append(String.format("%s %.2f hi\n", history.getDates()[i].toLocalTime(), hi.values[i]));
            }
            if (lo.values[i] > 0) {
                sb.append(String.format("%s %.2f lo\n", history.getDates()[i].toLocalTime(), lo.values[i]));
            }
        }
        print(sb.toString(), ANSI_YELLOW);
    }

    void load(String s) {
        var optH = repository.get().load(s, false);
        if (optH.isEmpty()) return;
        history = optH.get();
        var vwap = history.vwap("vwap");
        var strat = history.strat("strat");
        for (PriceHistory.IndexEntry entry : history.index().entries()) {
            StringColourise.print("[yellow]%s[/]".formatted(entry.toString()));
        }
    }


    void info(int n) {
        PriceHistory.Index index = history.index();
        List<PriceHistory.IndexEntry> entries = index.entries();
        if (n < 0 || n >= entries.size()) {
            print("index out of range", ANSI_RED);
            return;
        }
        PriceHistory.IndexEntry entry = entries.get(n);
        var map = index.makeMessages(entry, n >= 1 ? entries.get(n - 1) : null);
        var sb = new StringBuilder();
        for (String s : map.reversed().values()) {
            sb.append(System.lineSeparator()).append(s);
        }
        print("\n---\ntrade date " + entry.tradeDate().toString(), ANSI_YELLOW);
        print(sb.toString(), ANSI_YELLOW);
        if (entry.rthStart() > 0) {
            var rollingStandardize = history.rolling_standardize("volume", 30, "volstd");
            var values = PriceHistory.standardize(history.getColumn("volume"), entry.rthStart(), entry.end() + 1);
            for (int i = entry.rthStart(); i < entry.end() + 1; i++) {
                if (rollingStandardize.values[i] > 2 || values[i - entry.rthStart()] > 2) {
                    print(history.bar(i).toString() + " " + values[i - entry.rthStart()] + " " + rollingStandardize.values[i], ANSI_CYAN);
                }
            }
        }
    }

    private static void print(String line, String escapeSeq) {
        System.out.println(escapeSeq + line + ANSI_RESET);
    }

}
