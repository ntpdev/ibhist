package ibhist;

import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.Decimal;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HistoricalDataActionTest {

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
    void test_process_captures_bars_and_formats() throws InterruptedException {
        BlockingQueue<Action> mockQueue = mock(BlockingQueue.class);

        var action = new HistoricalDataAction(null, new AtomicInteger(), mockQueue, new Contract(), Duration.DAY_1, false);
        action.process(newBar(4983d, 0));
        action.process(newBar(4984d, 1));

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

    private Bar newBar(double d, int mins) {
        var tm = "20230919 23:00:%02d Europe/London".formatted(mins);
        return new Bar(tm, d, d + 2, d - 1, d + 1, Decimal.get(2501), 100, Decimal.get(d + .033d));
    }
}