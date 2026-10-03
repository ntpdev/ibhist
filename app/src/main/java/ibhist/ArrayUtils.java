package ibhist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayUtils {

    /**
     * returns the indexes of swing highs in a centered window, matching the futdata
     * {@code _find_swings} algorithm.
     * <p>
     * An index i is a swing high if values[i] is strictly greater than every other
     * value within {@code windowSize / 2} bars on each side of i. Centers too close
     * to either edge of the start/end slice to fill a full half window are skipped,
     * and plateaus of equal prices produce no swing point.
     * <p>
     * Negative end values are treated as counting from the end = 0 is values.length
     */
    public static int[] localMax(double[] values, int start, int end, int windowSize) {
        return findSwingIndexes(values, start, end, windowSize, true);
    }

    /**
     * returns the indexes of swing lows in a centered window, matching the futdata
     * {@code _find_swings} algorithm.
     * <p>
     * An index i is a swing low if values[i] is strictly less than every other
     * value within {@code windowSize / 2} bars on each side of i. Centers too close
     * to either edge of the start/end slice to fill a full half window are skipped,
     * and plateaus of equal prices produce no swing point.
     * <p>
     * Negative end values are treated as counting from the end = 0 is values.length
     */
    public static int[] localMin(double[] values, int start, int end, int windowSize) {
        return findSwingIndexes(values, start, end, windowSize, false);
    }

    private static int[] findSwingIndexes(double[] values, int start, int end, int windowSize, boolean isHigh) {
        if (end <= 0) {
            end += values.length;
        }

        start = Math.max(0, start);
        end = Math.min(values.length, end);

        if (start >= end) {
            return new int[0];
        }

        int half = windowSize / 2;
        int[] results = new int[end - start];
        int n = 0;

        for (int i = start; i < end; i++) {
            if (i - half < start || i + half >= end) {
                continue;
            }

            double currentValue = values[i];
            boolean isExtreme = true;

            for (int j = i - half; j <= i + half; j++) {
                if (j != i && (isHigh ? values[j] >= currentValue : values[j] <= currentValue)) {
                    isExtreme = false;
                    break;
                }
            }

            if (isExtreme) {
                results[n++] = i;
            }
        }

        // Return only the filled portion of the array
        return Arrays.copyOf(results, n);
    }

    /**
     * result[i] is the mean over a rolling window ending at i.
     *
     * @param values
     * @param start  - negative values are counted from the end
     * @param end    - 0 and negative values are counted from the end
     * @param n      - window size
     * @return
     */
    public static double[] rollingMean(double[] values, int start, int end, int n) {
        if (start < 0) {
            start += values.length;
        }
        if (end <= 0) {
            end += values.length;
        }

        start = Math.max(0, start - 1);
        end = Math.min(values.length, end);

        var results = new double[end - start];
        double sum = 0.0;

        for (int i = start; i < end; i++) {
            sum += values[i];

            if (i >= start + n - 1) {
                results[i - start] = sum / n;
                sum -= values[i - n + 1];
            }
        }

        return results;
    }

    /**
     * rolling stardarize. result[i] is the standarised values[i] over a rolling window. The value is x100 and rounded
     * equivalent to pandas std()
     *
     * @param values
     * @param start  - negative values are counted from the end
     * @param end    - 0 and negative values are counted from the end
     * @param n      - window size
     * @return
     */
    public static double[] rollingStandardize(double[] values, int start, int end, int n) {
        if (start < 0) {
            start += values.length;
        }
        if (end <= 0) {
            end += values.length;
        }

        start = Math.max(0, start - 1);
        end = Math.min(values.length, end);

        var results = new double[end - start];
        var mean = rollingMean(values, start, end, n);

        for (int i = n - 1; i < end - start; i++) {
            int windowStart = start + i - n + 1;

            // Calculate standard deviation
            double variance = 0;
            for (int j = windowStart; j <= start + i; j++) {
                double diff = values[j] - mean[i];
                variance += diff * diff;
            }
            double stdDev = Math.sqrt(variance / (n - 1));

            // Standardize, multiply by 100, and round
            double standardized = stdDev == 0 ? 0 : (values[start + i] - mean[i]) / stdDev;
            results[i] = Math.round(standardized * 100.0);
        }

        return results;
    }

    /**
     * return an int[] where result[i] is a count of the number of consecutive values before i for which
     * the condition is true.
     *
     * @param values
     * @param start  - negative values are counted from the end
     * @param end    - 0 and negative values are counted from the end
     * @param compare - a function that compares two values
     * @return
     */
    public static int[] countPrior(double[] values, int start, int end, BiDoublePredicate compare) {
        int n = values.length;

        // Handle negative indices - count from end
        if (start < 0) {
            start += n;
        }
        if (end <= 0) {
            end += n;
        }

        // Validate and clamp bounds
        start = Math.max(0, Math.min(start, n));
        end = Math.max(start, Math.min(end, n));

        int sliceLength = end - start;
        int[] result = new int[sliceLength];
        int[] stack = new int[sliceLength];
        int stackSize = 0;

        for (int i = 0; i < sliceLength; i++) {
            int actualIndex = start + i;
            int count = 0;

            while (stackSize > 0 && compare.test(values[actualIndex], values[start + stack[stackSize - 1]])) {
                count += result[stack[--stackSize]] + 1;
            }

            result[i] = count;
            stack[stackSize++] = i;
        }

        return result;
    }

    /**
     * find swing points over the start/end slice using a centered window of
     * {@code windowSize} bars (half = windowSize / 2 each side). Swing highs are
     * detected on the highs array, swing lows on the lows array, and the result is
     * merged into one index-ordered list, matching the futdata get_m1_swings algorithm.
     */
    public static List<Swing> findSwings(double[] highs, double[] lows, int start, int end, int windowSize) {
        var hs = localMax(highs, start, end, windowSize);
        var ls = localMin(lows, start, end, windowSize);
        // Pre-allocate ArrayList with known capacity to avoid resizing
        List<Swing> swings = new ArrayList<>(hs.length + ls.length);

        // Add local highs
        for (int i = 0; i < hs.length; i++) {
            int index = hs[i];
            swings.add(new Swing(index, highs[index], PointType.LOCAL_HIGH));
        }

        // Add local lows
        for (int i = 0; i < ls.length; i++) {
            int index = ls[i];
            swings.add(new Swing(index, lows[index], PointType.LOCAL_LOW));
        }

        // Sort by index to maintain chronological order
        swings.sort((s1, s2) -> Integer.compare(s1.index(), s2.index()));

        return swings;
    }

    public enum PointType {
        LOCAL_HIGH,
        LOCAL_LOW
    }

    public record Swing(int index, double value, PointType type) {
    }
}
