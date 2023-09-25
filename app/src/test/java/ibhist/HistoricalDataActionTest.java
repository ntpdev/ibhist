package ibhist;

import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.Decimal;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HistoricalDataActionTest {

    @Test
    void test_process_captures_bars_and_formats() {
        Consumer<HistoricalDataAction> mockConsumer = mock(Consumer.class);
        var h = new HistoricalDataAction(null, new AtomicInteger(), new Contract(), mockConsumer);
        h.process(newBar(4983d));
        h.process(newBar(4984d));

        h.process();

        verify(mockConsumer).accept(eq(h));
        assertThat(h.getBars()).hasSize(2);
        assertThat(h.getBarsAsCsv()).hasSize(3);
    }

    private Bar newBar(double d) {
        return new Bar("a", d, d + 2, d - 1, d + 1, Decimal.get(2501), 100, Decimal.get(d+.033d));
    }
}