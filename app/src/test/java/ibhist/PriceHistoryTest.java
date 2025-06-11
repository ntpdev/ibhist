package ibhist;

import com.ib.client.Bar;
import com.ib.client.Decimal;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static ibhist.StringUtils.print;
import static org.assertj.core.api.Assertions.assertThat;

class PriceHistoryTest {
    static final String INPUT = "input";
    static final String OUTPUT = "output";

    @Test
    void expand_doubles_size() {
        double[] xs = {7.0, 6.0, 5.0, 4.0, 3.0};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        var c = ph.setColumnValues(INPUT, xs);
        assertThat(c).startsWith(xs);

        ph.expand();

        var d = ph.getColumn(INPUT);
        assertThat(d).isNotSameAs(c);
        assertThat(d).hasSize(xs.length * 2);
        assertThat(d).startsWith(xs);
    }

    @Test
    void expands_on_insert() {
        var h = new PriceHistory("ES", 2, "date", "open", "high", "low", "close", "volume");
        int i = 0;
        h.insert(i++, LocalDateTime.of(2024, 3, 2, 14, 30), 8, 10, 7, 9, 100)
                .insert(i++, LocalDateTime.of(2024, 3, 2, 14, 31), 9, 11, 7, 10, 100);
        assertThat(h.length()).isEqualTo(2);
        h.insert(i++, LocalDateTime.of(2024, 3, 2, 14, 32), 8, 10, 7, 11, 100)
                .insert(i++, LocalDateTime.of(2024, 3, 2, 14, 33), 9, 11, 7, 12, 100);
        assertThat(h.length()).isEqualTo(4);
    }

    @Test
    void max_decreasing() {
        double[] xs = {7.0, 6.0, 5.0, 4.0, 3.0};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMax(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(7, 7, 7, 6, 5);
    }

    @Test
    void max_increasing() {
        double[] xs = {3.0, 4.0, 5.0, 6.0, 7.0};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMax(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(3, 4, 5, 6, 7);
    }

    @Test
    void max_equal_elements() {
        double[] xs = {1, 1, 1, 1, 1};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMax(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(1, 1, 1, 1, 1);
    }

    @Test
    void max_one_higher() {
        double[] xs = {1, 1, 1, 9, 1, 1, 1};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMax(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(1, 1, 1, 9, 9, 9, 1);
    }

    @Test
    void max_closes() {
        double[] xs = {1, 3, 5, 4, 2, 3, 1, 6, 7, 1};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMax(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(1, 3, 5, 5, 5, 4, 3, 6, 7, 7);
    }

    @Test
    void min_decreasing() {
        double[] xs = {7.0, 6.0, 5.0, 4.0, 3.0};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMin(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(7, 6, 5, 4, 3);
    }

    @Test
    void min_increasing() {
        double[] xs = {3.0, 4.0, 5.0, 6.0, 7.0};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMin(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(3, 3, 3, 4, 5);
    }

    @Test
    void min_equal_elements() {
        double[] xs = {1, 1, 1, 1, 1};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMin(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(1, 1, 1, 1, 1);
    }

    @Test
    void min_one_lower() {
        double[] xs = {9, 9, 9, 1, 9, 9, 9};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMin(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(9, 9, 9, 1, 1, 1, 9);
    }

    @Test
    void rolling_min() {
        double[] xs = {5, 3, 1, 4, 2, 3, 1, 6, 7, 3};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMin(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(5, 3, 1, 1, 1, 2, 1, 1, 1, 3);
    }

    @Test
    void local_max() {
        double[] xs = {1, 2, 3, 2, 1, 2, 3, 2, 1, 10};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.localMax(xs, 0, -1, 2);
        assertThat(ys).containsExactly(2, 6, 9);
    }

    @Test
    void local_min() {
        double[] xs = {5, 4, 3, 4, 5, 4, 3, 4, 5, 1};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.localMin(INPUT, 2, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(0, 0, 3, 0, 0, 0, 3, 0, 0, 1);
    }

    @Test
    void ema_step() {
        double[] xs = {100, 100, 100, 100, 200, 200, 200, 200};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.ema(INPUT, OUTPUT, 4); // w = 2/(4 + 1) = 0.4
        assertThat(ys.values.length).isEqualTo(xs.length);
        double[] expected = {100, 100, 100, 100, 140, 164, 178.4, 187.04};
        assertThat(ys.values).containsExactly(expected, Offset.offset(1e-5));
    }

    @Test
    void ema_fills_sma() {
        double[] xs = {90, 95, 105, 110, 200, 200, 200, 200};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.ema(INPUT, OUTPUT, 4); // w = 2/(4 + 1) = 0.4
        assertThat(ys.values.length).isEqualTo(xs.length);
        double[] expected = {100, 100, 100, 100, 140, 164, 178.4, 187.04};
        assertThat(ys.values).containsExactly(expected, Offset.offset(1e-5));
    }

    @Test
    void hilo_count() {
        double[] xs = {1, 3, 5, 4, 2, 3, 1, 6, 7, 1};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);

        var ys = ph.hilo(INPUT, OUTPUT);

        assertThat(ys.values).containsExactly(0, 1, 2, -1, -3, 1, -5, 7, 8, -2);
    }

    @Test
    void hilo_equal() {
        double[] xs = {1, 3, 3, 3, 5};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);

        var ys = ph.hilo(INPUT, OUTPUT);

        assertThat(ys.values).containsExactly(0, 1, 0, 0, 4);
    }

    @Test
    void hilo_down() {
        double[] xs = {7, 5, 3, 6, 4, 2};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);

        var ys = ph.hilo(INPUT, OUTPUT);

        assertThat(ys.values).containsExactly(0, -1, -2, 2, -1, -5);
    }

    @Test
    void test_rising_trend() {
        var history = test_price_history(128, 0.01);
        var hilo = history.hilo("close", "hilo");
        assertThat(hilo.values.length).isEqualTo(128);
        assertThat(hilo.values[16]).isEqualTo(16);
        assertThat(hilo.values[17]).isEqualTo(0);
        assertThat(hilo.values[18]).isEqualTo(-2);
        assertThat(hilo.values[79]).isEqualTo(79);
        assertThat(hilo.values[80]).isEqualTo(0);
        assertThat(hilo.values[81]).isEqualTo(-2);

        var b = history.aggregrate(120, 127);
        assertThat(b.open()).isEqualTo(114.25, Offset.offset(1e-6));
        assertThat(b.close()).isEqualTo(135.00, Offset.offset(1e-6));
        assertThat(b.volume()).isEqualTo(800);

        var highs = history.getColumn("high");
        var lmax = history.localMax(highs, 0, -1, 5);
        assertThat(lmax).containsExactly(16,17,79,80, 127);

//        assertThat(lmax.values.length).isEqualTo(128);
//        assertThat(lmax.values[16]).isEqualTo(130);
//        assertThat(lmax.values[17]).isEqualTo(130);
//        assertThat(lmax.values[18]).isEqualTo(0);
//        assertThat(lmax.values[79]).isEqualTo(145.75);
//        assertThat(lmax.values[80]).isEqualTo(145.75);
//        assertThat(lmax.values[81]).isEqualTo(0);
//        var cl = history.getColumn("close");
//        for (int i = 0; i < 128; i++) {
//            print("%s %.2f %.0f %.2f", history.dates[i].toLocalTime(),  cl[i], hilo.values[i], lmax.values[i]);
//        }
    }

    @Test
    void test_load_ib_bars() {
        List<Bar> bars = List.of(
                new Bar("20230919 23:00:00 Europe/London", 21.25, 25.75, 19.00, 22.50, Decimal.get(123.45d), 13, Decimal.get(23.45d)));
        var history = PriceHistory.createFromIBBars("test", bars);

        assertThat(history.length()).isEqualTo(1);
        assertThat(history.getDates()[0]).isEqualTo(LocalDateTime.of(2023, 9, 19, 23, 0, 0));
    }

    // n = 128 skew 0.01
    // generate sine wave price history with skew starts at 14:30 so Index entries not valid
    private PriceHistory test_price_history(int n, double skew) {
        LocalDateTime dt = LocalDate.now().atTime(14, 30);
        var history = new PriceHistory("ES", 128, "date", "open", "high", "low", "close", "volume");
        double open = 100;
        for (int i = 0; i < n; i++) {
            double d = generatePrice(i, skew);
            history.add(dt.plusMinutes(i), open, d + 1, d - 1, d, 100);
            open = d;
        }
        history.vwap("vwap");
        history.strat("strat");
        return history;
    }

    record Point(int index, double value, int count) {
    }

    // sin curve between 75 and 125 with 0.25 steps
    double generatePrice(int x, double skew) {
        return 100d + Math.round(100 * (skew * x + Math.sin(x * 0.1))) / 4d;
    }

    //    Bar generateBar(int i) {
//        LocalDateTime dt = LocalDate.now().atTime(14, 30);
//        return new Bar()
//    }

}