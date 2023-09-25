package ibhist;

import com.ib.client.Bar;
import com.ib.client.Decimal;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PriceHistoryRepositoryTest {

    @Test
    void test_loadSingleDay() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), "esu3", ".csv");
        var history = repository.load(Path.of("c:\\temp\\ultra\\esz3"));
        assertThat(history.length()).isEqualTo(1346);
        var index = history.makeIndex();
        assertThat(index.getIndexEntries()).hasSize(1);
    }

    @Test
    void test_load_ib_bars() {
        List<Bar> bars = List.of(
                new Bar("20230919 23:00:00 Europe/London", 21.25, 25.75, 19.00, 22.50, Decimal.get(123.45d), 13, Decimal.get(23.45d)));
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), "esu3", ".csv");

        var history = repository.loadFromIBBars(bars);

        assertThat(history.length()).isEqualTo(1);
        assertThat(history.getDates()[0]).isEqualTo(LocalDateTime.of(2023, 9, 19, 23, 0, 0));
    }
}