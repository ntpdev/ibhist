package ibhist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayUtils {

    /**
     * returns the indexes of local maxima in a centered sliding window of size 2n + 1
     * An index i is in the list if values[i] >= all items in window
     * Negative end values are treated as counting from the end = 0 is values.length
     */
    public static int[] localMax(double[] values, int start, int end, int windowSize) {
        if (end <= 0) {
            end += values.length;
        }

        start = Math.max(0, start - 1);
        end = Math.min(values.length, end);

        if (start >= end) {
            return new int[0];
        }

        int[] results = new int[end - start];
        int n = 0;

        for (int i = start; i < end; i++) {
            // Define window bounds
            int windowStart = Math.max(0, i - windowSize);
            int windowEnd = Math.min(values.length - 1, i + windowSize);

            boolean isLocalMax = true;
            double currentValue = values[i];

            // Check if current value is >= all values in window
            for (int j = windowStart; j <= windowEnd; j++) {
                if (values[j] > currentValue) {
                    isLocalMax = false;
                    break;
                }
            }

            if (isLocalMax) {
                results[n++] = i;
            }
        }

        // Return only the filled portion of the array
        return Arrays.copyOf(results, n);
    }

    /**
     * returns the indexes of local minima in a sliding window of size 2n + 1
     * An index i is in the list if values[i] <= all items in window
     * Negative end values are treated as counting from the end = 0 is values.length
     */
    public static int[] localMin(double[] values, int start, int end, int windowSize) {
        if (end <= 0) {
            end += values.length;
        }

        start = Math.max(0, start - 1);
        end = Math.min(values.length, end);

        if (start >= end) {
            return new int[0];
        }

        int[] results = new int[end - start];
        int n = 0;

        for (int i = start; i < end; i++) {
            // Define window bounds
            int windowStart = Math.max(0, i - windowSize);
            int windowEnd = Math.min(values.length - 1, i + windowSize);

            boolean isLocalMin = true;
            double currentValue = values[i];

            // Check if current value is <= all values in window
            for (int j = windowStart; j <= windowEnd; j++) {
                if (values[j] < currentValue) {
                    isLocalMin = false;
                    break;
                }
            }

            if (isLocalMin) {
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
