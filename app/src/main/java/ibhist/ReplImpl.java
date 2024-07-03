package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;

public class ReplImpl implements Repl {
    private static final Logger log = LogManager.getLogger("ReplImpl");
    static final Splitter WS_SPLITTER = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings();
    static final String ANSI_RESET = "\u001B[0m";
    static final String ANSI_GREEN = "\u001B[32m";
    static final String ANSI_YELLOW = "\u001B[33m";
    static final String ANSI_CYAN = "\u001B[36m";
    private final IBConnector connector;
    private PriceHistoryRepository repository = null;
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
        print("enter command [x|load sym|info n]", ANSI_YELLOW);
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> input = WS_SPLITTER.splitToList(line);
            print(line, ANSI_YELLOW);
            if (input.get(0).equalsIgnoreCase("x")) {
                break;
            } else if (input.get(0).equalsIgnoreCase("load") && input.size() > 1) {
                load(input.get(1));
            } else if (input.get(0).equalsIgnoreCase("info") && input.size() > 1 && history != null) {
                info(Integer.parseInt(input.get(1)));
            } else if (input.get(0).equalsIgnoreCase("ib")) {
                connector.process(true);
            } else if (input.get(0).equalsIgnoreCase("p") && input.size() > 1) {
                printHistory(Integer.parseInt(input.get(1)));
            }
        }
        reader.close();
    }

    private void printHistory(int n) {
        print(history.debugPrint(n), ANSI_GREEN);
    }

    void load(String s) {
        repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        history = repository.load(s);
        var vwap = history.vwap("vwap");
        var strat = history.strat("strat");
        for (PriceHistory.IndexEntry entry : history.index().entries()) {
            print(entry.toString(), ANSI_YELLOW);
        }
    }

    void info(int n) {
        PriceHistory.Index index = history.index();
        List<PriceHistory.IndexEntry> entries = index.entries();
        if (n >= 0 && n < entries.size()) {
            PriceHistory.IndexEntry entry = entries.get(n);
            var map = index.makeMessages(entry, n >= 1 ? entries.get(n - 1) : null);
            var sb = new StringBuilder();
            for (String s : map.reversed().values()) {
                sb.append(System.lineSeparator()).append(s);
            }
            print("\n---\ntrade date " + entry.tradeDate().toString(), ANSI_YELLOW);
            print(sb.toString(), ANSI_YELLOW);
        }
    }

    private static void print(String line, String escapeSeq) {
        System.out.println(escapeSeq + line + ANSI_RESET);
    }

}
