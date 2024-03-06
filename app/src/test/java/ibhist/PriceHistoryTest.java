package ibhist;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PriceHistoryTest {
    static final String INPUT = "input";
    static final String OUTPUT = "output";

    @Test
    void expand_doubles_size() {
        double[] xs = {7.0, 6.0, 5.0, 4.0, 3.0};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        ph.expand();
        var c = ph.getColumn(INPUT);
        assertThat(c).hasSize(xs.length * 2);
        assertThat(c).startsWith(xs);
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
    void min_closes() {
        double[] xs = {5, 3, 1, 4, 2, 3, 1, 6, 7, 3};
        var ph = new PriceHistory("ES", xs.length, INPUT);
        ph.setLength(xs.length);
        ph.setColumnValues(INPUT, xs);
        var ys = ph.rollingMin(INPUT, 3, OUTPUT);
        assertThat(ys.values.length).isEqualTo(xs.length);
        assertThat(ys.values).containsExactly(5, 3, 1, 1, 1, 2, 1, 1, 1, 3);
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
}