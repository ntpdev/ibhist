package ibhist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.DoublePredicate;
import java.lang.Math;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;

/**
 * Class to manage a list of MonitorData objects with add/remove functionality
 * Implements Iterable to support for-each loops
 */
public class MonitorManager implements Iterable<MonitorManager.MonitorData> {
    private final List<MonitorData> monitors = new ArrayList<>();

    /**
     * Parse input command and update monitor list accordingly
     *
     * @param input Command string like "add monitor > 1234.56 3" or "del monitor"
     * @return true if command was processed successfully, false otherwise
     */
    public boolean processCommand(String input) {
        if (input == null) {
            return false;
        }

        return processCommand(StringUtils.split(input));
    }

    public boolean processCommand(List<String> parts) {
        if (parts.size() < 2 || !parts.get(1).equalsIgnoreCase("monitor")) {
            return false;
        }

        String command = parts.get(0).toLowerCase();
        return switch (command) {
            case "add" -> handleAddCommand(parts);
            case "del" -> handleDelCommand();
            default -> false;
        };
    }

    private boolean handleAddCommand(List<String> parts) {
        // Expected: add monitor <condition> <float> [<integer>]
        // Example: add monitor > 1234.56 3
        if (parts.size() < 4) {
            return false;
        }

        try {
            int count = parts.size() == 4 ? 1 : Integer.parseInt(parts.get(4));
            monitors.add(new MonitorData(
                    parts.get(2) + " " + parts.get(3),
                    parseCondition(parts.get(2), parts.get(3)),
                    count));
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean handleDelCommand() {
        if (monitors.isEmpty()) {
            return false;
        }
        monitors.removeLast();
        return true;
    }

    /**
     * Get an unmodifiable view of the monitor list
     */
    public List<MonitorData> getMonitors() {
        return Collections.unmodifiableList(monitors);
    }

    /**
     * Get the number of monitors
     */
    public int size() {
        return monitors.size();
    }

    /**
     * Clear all monitors
     */
    public void clear() {
        monitors.clear();
    }

    /**
     * Returns an iterator over the monitors for use in for-each loops
     */
    @Override
    public Iterator<MonitorData> iterator() {
        return monitors.iterator();
    }

    /**
     * Parse operator and number string into a predicate function
     *
     * @param op     Operator string like ">", "<", ">=", "<=", "==", "="
     * @param number Number string like "1234.56"
     * @return DoublePredicate that tests the condition
     */
    public static DoublePredicate parseCondition(String op, String number) {
        double threshold = Double.parseDouble(number.trim());

        return switch (op.trim()) {
            case ">=" -> d -> d >= threshold;
            case "<=" -> d -> d <= threshold;
            case ">" -> d -> d > threshold;
            case "<" -> d -> d < threshold;
            case "==", "=" -> d -> Double.compare(d, threshold) == 0;
            default -> throw new IllegalArgumentException("Invalid operator: " + op);
        };
    }

    @Override
    public String toString() {
        return "MonitorManager size=" + size() +
                " [" +
                Joiner.on(", ").join(monitors.stream().map(MonitorData::condition).collect(Collectors.toList())) +
                "]";
    }

    /**
     * Record to hold monitor data with condition string, predicate function, and integer threshold
     */
    public record MonitorData(
            String condition,
            DoublePredicate predicate,
            int threshold) {

        public MonitorData {
            Objects.requireNonNull(predicate);
            threshold = Math.max(threshold, 1);
        }

        public boolean test(double d) {
            return predicate.test(d);
        }

        public boolean meetsThreshold(int count) {
            return count >= this.threshold;
        }
    }
}
