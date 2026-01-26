package ibhist;

import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.Decimal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HistoricalDataActionTest {
    private static final Logger log = LogManager.getLogger(HistoricalDataActionTest.class.getSimpleName());
    private static final ZoneId IB_ZONE = ZoneId.of("Europe/London");
    // yyyyMMdd HH:mm:ss <zone-id>
    private static final DateTimeFormatter IB_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");

    @Test
    void z() {
        var clock = LocalTime.of(13, 20, 15);
        var lastTm = clock;

        // Formatter for desired output
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        int c = 99;
        // Loop 25 times
        for (int n = 1; n <= 50; n++) {
            --c;
            var barTm = clock.minusSeconds(5);
            // Print the current row number and formatted time
            boolean update = lastTm.getMinute() == barTm.getMinute();
            if (!update) {
                c = 11;
            }
            if (c == 0) {
                System.out.println(n + " " + c + " " + clock.format(formatter) + " " + barTm.format(formatter) + " " + update);
            }
            lastTm = barTm;
            // Add 5 seconds to the current time
            clock = clock.plusSeconds(5);
        }
    }

    @Test
    void test_onHistoricalData_captures_bars_and_formats() throws InterruptedException {
        BlockingQueue<Action> mockQueue = mock(BlockingQueue.class);

        var action = new HistoricalDataAction(null, new AtomicInteger(), mockQueue, new Contract(), null, Duration.DAY_1, false, new MonitorManager());
        action.onHistoricalData(newBar(4983d, 0));
        action.onHistoricalData(newBar(4984d, 1));

        action.complete();

        verify(mockQueue).put(eq(action));
        assertThat(action.getBars()).hasSize(2);
        // text blocks use \n as line separator but the csv is created using the system line separator
        String csv = """
                ,Date,Open,High,Low,Close,Volume,WAP,BarCount
                0,20230919 23:00:00,4983.00,4985.00,4982.00,4984.00,2501,4983.033,100
                1,20230919 23:00:01,4984.00,4986.00,4983.00,4985.00,2501,4984.033,100
                """.replaceAll("\n", System.lineSeparator());
        assertThat(action.barsAsCsv()).isEqualTo(csv);
    }


    @Test
    void test_onHistoricalData() {
//        var monitor = new MonitorManager();
        BlockingQueue<Action> queue = new ArrayBlockingQueue<>(16);
        var contract = new Contract();
        contract.symbol("ES");
        var action = new HistoricalDataAction(null, new AtomicInteger(), queue, contract, null, Duration.DAY_1, false, new MonitorManager());
        var repo = new PriceHistoryRepository();
        var history = repo.load("esu").get();
        var idx = history.indexEntry(LocalDate.of(2025, 6, 18));
        var open = history.getColumn("open");
        var high = history.getColumn("high");
        var low = history.getColumn("low");
        var close = history.getColumn("close");
        var volume = history.getColumn("volume");
        for (int i = idx.start(); i < idx.rthStart(); ++i) {
            var b = new Bar(toIBDateTime(history.getDates()[i]), open[i], high[i], low[i], close[i], Decimal.get(volume[i]), 0, Decimal.get(0));
            action.onHistoricalData(b);
        }
        action.onHistoricalDataEnd();
        try {
            var result = queue.take();
            assertThat(result).isInstanceOf(HistoricalDataAction.class);
            var hda = (HistoricalDataAction) result;
            assertThat(hda.asPriceHistory().length()).isEqualTo(930);
            log.info(hda.asPriceHistory());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void test_onHistoricalDataUpdate() {
//        var monitor = new MonitorManager();
        BlockingQueue<Action> queue = new ArrayBlockingQueue<>(16);
        var monitorManager = new MonitorManager();
        monitorManager.processCommand("add monitor > 6042.50 3");
        var contract = new Contract();
        contract.symbol("ES");
        var action = new HistoricalDataAction(null, new AtomicInteger(), queue, contract, null, Duration.DAY_1, true, monitorManager);
        var repo = new PriceHistoryRepository();
        var history = repo.load("esu5").get();
        var idx = history.indexEntry(LocalDate.of(2025, 6, 18));
        var open = history.getColumn("open");
        var high = history.getColumn("high");
        var low = history.getColumn("low");
        var close = history.getColumn("close");
        var volume = history.getColumn("volume");
        for (int i = idx.start(); i < idx.rthStart(); ++i) {
            var b = new Bar(toIBDateTime(history.getDates()[i]), open[i], high[i], low[i], close[i], Decimal.get(volume[i]), 0, Decimal.get(0));
            action.onHistoricalData(b);
        }
        action.onHistoricalDataEnd();
        assertThat(queue).hasSize(0);

        // simulate realtime updates
        for (int i = idx.rthStart(); i < idx.rthStart() + 15; ++i) {
            var dt = history.getDates()[i];
            // send same bar 12 times to simulate 5s update freq
            var b = new Bar(toIBDateTime(dt), open[i], high[i], low[i], close[i], Decimal.get(volume[i]), 0, Decimal.get(0));
            for (int j = 12; j > 0; --j) {
                action.onHistoricalDataUpdate(b);
            }
        }
        // normally the action would call cancel() and put itself on the queue
        log.info(action.asPriceHistory());
    }

    private String toIBDateTime(LocalDateTime dt) {
        return dt.atZone(IB_ZONE).format(IB_FORMATTER);
    }

    private Bar newBar(double d, int mins) {
        var tm = "20230919 23:00:%02d Europe/London".formatted(mins);
        return new Bar(tm, d, d + 2, d - 1, d + 1, Decimal.get(2501), 100, Decimal.get(d + .033d));
    }
}