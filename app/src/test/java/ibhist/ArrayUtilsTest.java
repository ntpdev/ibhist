package ibhist;

import org.junit.jupiter.api.Test;

import static ibhist.ArrayUtils.PointType.LOCAL_HIGH;
import static ibhist.ArrayUtils.PointType.LOCAL_LOW;
import static org.assertj.core.api.Assertions.assertThat;

class ArrayUtilsTest {
    @Test
    void local_max_decreasing() {
        double[] xs = {5, 4, 3, 2, 1};
        var ys = ArrayUtils.localMax(xs, 0, 0, 2);
        assertThat(ys).containsExactly(0);
    }

    @Test
    void local_max() {
        double[] xs = {1, 2, 3, 2, 1, 2, 3, 2, 1, 10};
        var ys = ArrayUtils.localMax(xs, 0, 0, 2);
        assertThat(ys).containsExactly(2, 6, 9);
    }

    @Test
    void local_max_equal() {
        double[] xs = {1, 2, 3, 3, 3, 2, 1};
        var ys = ArrayUtils.localMax(xs, 0, 0, 2);
        assertThat(ys).containsExactly(2, 3, 4);
    }

    @Test
    void local_max_wave() {
        var xs = sine_wave();
        var ys = ArrayUtils.localMax(xs, 0, 0, 5);
        assertThat(ys).containsExactly(10, 50, 79);
    }

    @Test
    void local_min_decreasing() {
        double[] xs = {5, 4, 3, 2, 1};
        var ys = ArrayUtils.localMin(xs, 0, 0, 2);
        assertThat(ys).containsExactly(4);
    }

    @Test
    void local_min() {
        double[] xs = {5, 4, 3, 4, 5, 4, 3, 4, 5, 1};
        var ys = ArrayUtils.localMin(xs, 0, 0, 2);
        assertThat(ys).containsExactly(2, 6, 9);
    }

    @Test
    void local_min_equal() {
        double[] xs = {3, 2, 1, 1, 1, 2, 3, 4};
        var ys = ArrayUtils.localMin(xs, 0, 0, 2);
        assertThat(ys).containsExactly(2, 3, 4);
    }

    @Test
    void local_min_wave() {
        var xs = sine_wave();
        var ys = ArrayUtils.localMin(xs, 0, 0, 5);
        assertThat(ys).containsExactly(0, 30, 70);
    }

    @Test
    void rolling_mean() {
        double[] xs = {9, 6, 3, 3, 3, 6, 9, 12};
        var ys = ArrayUtils.rollingMean(xs, 0, 0, 3);
        assertThat(ys).containsExactly(0, 0, 6.0, 4.0, 3.0, 4.0, 6.0, 9.0);
    }

    @Test
    void rolling_standardize() {
        double[] xs = {9, 6, 3, 3, 3, 6, 9, 12};
        var ys = ArrayUtils.rollingStandardize(xs, 0, 0, 3);
        assertThat(ys).containsExactly(0.0, 0.0, -100.0, -58.0, 0.0, 115.0, 100.0, 100.0);
    }

    @Test
    void rolling_standardize_pandas() {
        /* equivalent pandas code
        c1 = dc["volume"].iloc[930:980]
        c2 = c1.rolling(window=20).mean()
        c3 = (((c1 - c2) / c1.rolling(window=20).std()) * 100).round()
         */
        double[] vol = {12540, 9727, 9506, 5560, 3908, 4303, 4010, 4421, 3883, 4260, 5483, 3017, 2133, 2430, 2004, 4847, 2773, 2667, 2825, 3851, 5843, 4515, 2491, 2779, 3989, 3759, 2706, 2776, 3168, 2413, 7769, 3238, 3466, 2960, 2321, 3169, 3105, 2121, 2985, 1673, 4747, 4540, 2914, 2323, 2559, 2377, 1974, 1883, 2394, 1770};
        double[] mean = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 4707.4, 4372.55, 4111.95, 3761.2, 3622.15, 3626.2, 3599.0, 3533.8, 3451.55, 3415.8, 3323.45, 3437.75, 3448.8, 3515.45, 3541.95, 3557.8, 3473.9, 3490.5, 3463.2, 3471.2, 3362.3, 3307.5, 3308.75, 3329.9, 3307.1, 3235.6, 3166.5, 3129.9, 3085.25, 3046.55, 3014.4};
        double[] nvol = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -31.0, 70.0, 24.0, -108.0, -76.0, 33.0, 15.0, -74.0, -61.0, -22.0, -82.0, 306.0, -15.0, -4.0, -43.0, -92.0, -23.0, -30.0, -101.0, -37.0, -122.0, 111.0, 95.0, -32.0, -76.0, -52.0, -60.0, -87.0, -89.0, -48.0, -90.0};

        var ys = ArrayUtils.rollingMean(vol, 0, 0, 20);
        assertThat(ys).containsExactly(mean);

        var zs = ArrayUtils.rollingStandardize(vol, 0, 0, 20);
        assertThat(zs).containsExactly(nvol);
    }

    @Test
    void count_prior_low() {
        double[] xs = {1, 3, 5, 4, 2, 3, 1, 6, 7, 1};

        var ys = ArrayUtils.countPrior(xs, 0, 0, (a, b) -> a > b);

        assertThat(ys).containsExactly(0, 1, 2, 0, 0, 1, 0, 7, 8, 0);
    }


    @Test
    void count_prior_high() {
        double[] xs = {1, 3, 5, 4, 2, 3, 1, 6, 7, 1};

        var ys = ArrayUtils.countPrior(xs, 0, 0, (a, b) -> a < b);

        assertThat(ys).containsExactly(0, 0, 0, 1, 3, 0, 5, 0, 0, 2);
    }


    @Test
    void find_swings() {
        var highs = sine_wave();
        var lows = sine_wave();
        var xs = ArrayUtils.findSwings(highs, lows, 0, 0, 5);
        assertThat(xs).extracting(ArrayUtils.Swing::index).containsExactly(0, 10, 30, 50, 70, 79);
        assertThat(xs).extracting(ArrayUtils.Swing::type).containsExactly(LOCAL_LOW, LOCAL_HIGH, LOCAL_LOW, LOCAL_HIGH, LOCAL_LOW, LOCAL_HIGH);
    }

    double[] sine_wave() {
        double[] xs = new double[80];
        for (int i = 0; i < xs.length; i++) {
            xs[i] = 100 + Math.round(100 * Math.sin((i * Math.PI * 2) / 40.0));
        }
        return xs;
    }
}