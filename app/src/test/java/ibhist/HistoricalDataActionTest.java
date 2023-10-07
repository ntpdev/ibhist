package ibhist;

import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.Decimal;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HistoricalDataActionTest {

    @Test
    void test_process_captures_bars_and_formats() throws InterruptedException {
        BlockingQueue<Action> mockQueue = mock(BlockingQueue.class);
        var h = new HistoricalDataAction(null, new AtomicInteger(), mockQueue, new Contract(), Duration.D1);
        h.process(newBar(4983d));
        h.process(newBar(4984d));

        h.process();

        verify(mockQueue).put(eq(h));
        assertThat(h.getBars()).hasSize(2);
        // text blocks use \n as line separator but the csv is created using the system line separator
        String csv = """
                ,Date,Open,High,Low,Close,Volume,WAP,BarCount
                0,20230919 23:00:00,4983.00,4985.00,4982.00,4984.00,2501,4983.033,100
                1,20230919 23:00:00,4984.00,4986.00,4983.00,4985.00,2501,4984.033,100
                """.replaceAll("\n", System.lineSeparator());
        assertThat(h.getBarsAsCsv()).isEqualTo(csv);
    }

    private Bar newBar(double d) {
        return new Bar("20230919 23:00:00 Europe/London", d, d + 2, d - 1, d + 1, Decimal.get(2501), 100, Decimal.get(d+.033d));
    }
}