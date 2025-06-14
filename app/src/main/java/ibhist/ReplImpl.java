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
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static ibhist.StringUtils.print;

public class ReplImpl implements Repl {
    private static final Logger log = LogManager.getLogger(ReplImpl.class.getSimpleName());
    private static final DateTimeFormatter tradeDateformatter = DateTimeFormatter.ofPattern("EEE d MMMM");
    private final IBConnector connector;
    private final TimeSeriesRepository tsRepository;
//    private final Supplier<PriceHistoryRepository> repository = Suppliers.memoize(() -> new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv"));
    private final Supplier<PriceHistoryRepository> repository = Suppliers.memoize(PriceHistoryRepository::new);
    private PriceHistory history = null;


    @Inject
    public ReplImpl(IBConnector connector, TimeSeriesRepository repository) {
        this.connector = connector;
        this.tsRepository = repository;
    }

    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            log.error("ReplIml unhandled exception", e);
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
                String noun = input.size() >= 2 ? input.get(1).toLowerCase() : "";
                if (cmd.equals("x")) {
                    processTws(List.of("disc"));
                    break;
                } else if (cmd.equals("load")) {
                    load(noun);
                } else if (cmd.equals("info") && history != null) {
                    info(parseInt(noun, -1));
                } else if (noun.equals("tws")) { // conn disc
                    processTws(input);
                } else if (noun.equals("es")) { // show stream end-stream
                    requestHistoricData(cmd);
                } else if (cmd.equals("rt")) {
                    processRt(input, monitorManager);
                } else if (cmd.equals("minmax") && history != null) {
                    printMinMax(parseInt(noun, 15));
                } else if (cmd.equals("p") && history != null) {
                    // param 1 = offset, -offset, hh:mm
                    if (noun.contains(":")) {
                        PriceHistory.Index index = history.index();
                        var entries = index.entries();
                        tryParseTime(noun, entries.getLast().tradeDate()).ifPresent(barTime -> {
                            int i = history.find(barTime);
                            if (i > 0) {
                                printHistory(i - 9, i + 10);
                            }
                        });
                    } else {
                        tryParseInt(noun).ifPresent(this::printHistory);
                    }
                } else {
                    // process "add monitor > 5924.25"
                    monitorManager.processCommand(input);
                }
            }
        }
        reader.close();
    }

    private boolean requestHistoricData(String cmd) {
        return switch (cmd) {
            case "show" -> {
                var action = connector.getHistoricalData("ES", IBConnectorImpl.CONTRACT_MONTH, Duration.DAY_1, false);
                history = action.asPriceHistory();
                print(history.toString());
                var path = action.save(history.index().entries().getFirst().tradeDate());
                tsRepository.append(history);
                print(history.toString());
                print(history.intradayPriceInfo(-1));
                yield true;
            }
            case "stream" -> {
                connector.requestHistoricalData("ES", IBConnectorImpl.CONTRACT_MONTH, Duration.DAY_1, true);
                yield true;
            }
            case "end-stream" -> {
                print(connector.actionsToString());
                connector.findByType(HistoricalDataAction.class)
                        .ifPresent(a -> {
                            var action = connector.waitForHistoricalData();
                            history = action.asPriceHistory();
                            print(history.toString());
                            var path = action.save(history.index().entries().getFirst().tradeDate());
                        });
                yield true;
            }
            default -> false;
        };
    }

    private boolean processTws(List<String> input) {
        if (input.getFirst().startsWith("conn"))
            connector.connect();
        else if (input.getFirst().startsWith("disc"))
            connector.disconnect();
        return true;
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
        var ix = history.indexEntry(-1);
        var swings = ArrayUtils.findSwings(history.getColumn("high"), history.getColumn("low"), ix.start(), ix.end(), n);

        StringBuilder sb = new StringBuilder();
        sb.append("--- minmax ").append(n).append("\n");
        for (var s : swings) {
            String col = s.type() == ArrayUtils.PointType.LOCAL_HIGH ? "[green]" : "[red]";
            sb.append(col).append(history.bar(s.index()).asIntradayBar()).append("[/]\n");
        }
        print(sb.toString());
        var vol = history.getColumn("volume");
        var stdvol = ArrayUtils.rollingStandardize(vol, 0, 0, 20);
        var high = history.getColumn("high");
        var low = history.getColumn("low");
        int[] countHighs = ArrayUtils.countPrior(high, 0, 0, (a, b) -> a > b);
        int[] countLows = ArrayUtils.countPrior(low, 0, 0, (a, b) -> a < b);
        print("hello");
        // times ~15:30
        for (int i = ix.rthStart() + 35; i < ix.rthStart() + 85; ++i) {
            log.info("%2d %s %.2f %.2f %d %d".formatted(i, history.dates[i], vol[i], stdvol[i], countHighs[i], countLows[i]));
        }
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
        var sb = new StringBuilder();
        String formattedDate = entry.tradeDate().format(tradeDateformatter.withLocale(Locale.ENGLISH));
        sb.append("\n[yellow]---\ntrade date ").append(formattedDate).append("[/]\n");
        print(sb);
        print(history.intradayPriceInfo(n));
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
