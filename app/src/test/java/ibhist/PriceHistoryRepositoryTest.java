package ibhist;

import com.ib.client.Bar;
import com.ib.client.Decimal;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PriceHistoryRepositoryTest {

    @Test
    void test_loadSingleDay() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), "esu3", ".csv");
        var history = repository.load(Path.of("c:\\temp\\ultra\\esz3"));
        assertThat(history.length()).isEqualTo(13800);
        var c = history.vwap("vwap");
        assertThat(c.values.length).isEqualTo(history.length());
        var index = history.makeIndex();
        assertThat(index.entries()).hasSize(10);
        var rthBars = index.rthBars();
        assertThat(rthBars).hasSize(10);
        for (PriceHistory.Bar bar : rthBars) {
            System.out.println(bar.asDailyBar());
        }
        var minVol = index.minVolBars(2500d);
        save(minVol);
    }

    private static void save(List<PriceHistory.Bar> minVol) {
        try {
            var buffer = new StringBuffer();
            buffer.append(",Date,DateCl,Open,High,Low,Close,Volume,VWAP").append(System.lineSeparator());
            for (int i = 0; i < minVol.size(); i++) {
                PriceHistory.Bar bar = minVol.get(i);
                buffer.append(String.format("%d,%s,%s,%.2f,%.2f,%.2f,%.2f,%.1f,%.2f%n", i, bar.start(), bar.end(), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume(), bar.vwap()));
            }
            Files.writeString(Path.of("c:\\temp\\ultra\\es-minvol.csv"), buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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