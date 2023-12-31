package ibhist;

import com.ib.client.Bar;
import com.ib.client.Decimal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PriceHistoryRepositoryTest {
    private static final Logger log = LogManager.getLogger("PriceHistoryRepositoryTest");

    @Test
    void test_loadAllDays() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        var history = repository.load("esz3");
        var c = history.vwap("vwap");
        assertThat(c.values.length).isEqualTo(history.length());
        LocalDateTime date = history.getDates()[9420];
        int pos = history.find(date);
        assertThat(pos).isEqualTo(9420);
        var b = history.bar(pos);
        pos = history.find(date.minusMinutes(1));
        assertThat(pos).isLessThan(0);
        pos = history.floor(date.minusMinutes(1));
        assertThat(pos).isEqualTo(9419);
//        TimeSeriesRepository tsr = new TimeSeriesRepository("mongodb://localhost:27017");
//        tsr.append(history);
        var rthBars = history.rthBars();
        for (PriceHistory.Bar bar : rthBars) {
            log.info(bar.asDailyBar());
        }
        var minVol = history.minVolBars(2500d);
        save(minVol);
    }

    @Test
    void test_loadSingleDay() {
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");
        var history = repository.load("ESZ3", Path.of("c:\\temp\\ultra\\ESZ3 20230918.csv"));
        assertThat(history.length()).isEqualTo(13800);
        var c = history.vwap("vwap");
        assertThat(c.values.length).isEqualTo(history.length());
        var rthBars = history.rthBars();
        assertThat(rthBars).hasSize(10);
        for (PriceHistory.Bar bar : rthBars) {
            System.out.println(bar.asDailyBar());
        }
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
        var repository = new PriceHistoryRepository(Path.of("c:\\temp\\ultra"), ".csv");

        var history = repository.loadFromIBBars("test", bars);

        assertThat(history.length()).isEqualTo(1);
        assertThat(history.getDates()[0]).isEqualTo(LocalDateTime.of(2023, 9, 19, 23, 0, 0));
    }
}