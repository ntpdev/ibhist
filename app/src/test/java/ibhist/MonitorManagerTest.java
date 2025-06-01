package ibhist;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class MonitorManagerTest {

    @Test
    void for_each_loop_iteration() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor > 100.0 1");
        manager.processCommand("add monitor < 200.0 2");
        manager.processCommand("add monitor >= 150.0 3");

        List<String> conditions = new ArrayList<>();
        List<Integer> values = new ArrayList<>();

        // Test for-each loop
        for (var monitor : manager) {
            conditions.add(monitor.condition());
            values.add(monitor.threshold());
        }

        assertThat(conditions).containsExactly("> 100.0", "< 200.0", ">= 150.0");
        assertThat(values).containsExactly(1, 2, 3);
    }


    @Test
    void add_monitor_with_greater_than_condition() {
        var manager = new MonitorManager();
        boolean result = manager.processCommand("add monitor > 1234.56 3");

        assertThat(result).isTrue();
        assertThat(manager.size()).isEqualTo(1);

        var monitor = manager.getMonitors().getFirst();
        assertThat(monitor.condition()).isEqualTo("> 1234.56");
        assertThat(monitor.threshold()).isEqualTo(3);
        assertThat(monitor.predicate().test(1235.0)).isTrue();
        assertThat(monitor.predicate().test(1234.0)).isFalse();
    }

    @Test
    void add_monitor_with_less_than_condition() {
        var manager = new MonitorManager();
        boolean result = manager.processCommand("add monitor < 100.5 7");

        assertThat(result).isTrue();
        assertThat(manager.size()).isEqualTo(1);

        var monitor = manager.getMonitors().getFirst();
        assertThat(monitor.condition()).isEqualTo("< 100.5");
        assertThat(monitor.threshold()).isEqualTo(7);
        assertThat(monitor.predicate().test(99.0)).isTrue();
        assertThat(monitor.predicate().test(101.0)).isFalse();
    }

    @Test
    void add_multiple_monitors() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor > 1234.56 3");
        manager.processCommand("add monitor < 500.0 5");
        manager.processCommand("add monitor >= 200.0 2");

        assertThat(manager.size()).isEqualTo(3);

        var monitors = manager.getMonitors();
        assertThat(monitors.getFirst().condition()).isEqualTo("> 1234.56");
        assertThat(monitors.get(1).condition()).isEqualTo("< 500.0");
        assertThat(monitors.get(2).condition()).isEqualTo(">= 200.0");
    }

    @Test
    void delete_last_monitor() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor > 1234.56 3");
        manager.processCommand("add monitor < 500.0 5");

        assertThat(manager.size()).isEqualTo(2);

        boolean result = manager.processCommand("del monitor");

        assertThat(result).isTrue();
        assertThat(manager.size()).isEqualTo(1);
        assertThat(manager.getMonitors().getFirst().condition()).isEqualTo("> 1234.56");
    }

    @Test
    void return_false_when_deleting_from_empty_list() {
        var manager = new MonitorManager();
        boolean result = manager.processCommand("del monitor");

        assertThat(result).isFalse();
        assertThat(manager.size()).isEqualTo(0);
    }

    @Test
    void default_count() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor > 1234.56");
        assertThat(manager.getMonitors().getFirst().threshold()).isEqualTo(1);
    }

    @Test
    void return_false_for_invalid_add_command_format() {
        var manager = new MonitorManager();
        assertThat(manager.processCommand("add monitor 1234.56 3")).isFalse(); // missing operator
        assertThat(manager.processCommand("add monitor > abc 3")).isFalse(); // invalid float
        assertThat(manager.processCommand("add monitor > 1234.56 abc")).isFalse(); // invalid integer
        assertThat(manager.size()).isEqualTo(0);
    }

    @Test
    void return_false_for_invalid_commands() {
        String s = null;
        var manager = new MonitorManager();

        assertThat(manager.processCommand("")).isFalse();
        assertThat(manager.processCommand(s)).isFalse();
        assertThat(manager.processCommand("invalid command")).isFalse();
        assertThat(manager.processCommand("add something > 123 4")).isFalse();
        assertThat(manager.processCommand("del something")).isFalse();
    }

    @Test
    void handle_various_comparison_operators() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor >= 100.0 1");
        manager.processCommand("add monitor <= 200.0 2");
        manager.processCommand("add monitor == 150.0 3");

        assertThat(manager.size()).isEqualTo(3);

        var monitors = manager.getMonitors();

        // Test >= condition
        assertThat(monitors.getFirst().predicate().test(100.0)).isTrue();
        assertThat(monitors.getFirst().predicate().test(99.9)).isFalse();

        // Test <= condition
        assertThat(monitors.get(1).predicate().test(200.0)).isTrue();
        assertThat(monitors.get(1).predicate().test(200.1)).isFalse();

        // Test == condition
        assertThat(monitors.get(2).predicate().test(150.0)).isTrue();
        assertThat(monitors.get(2).predicate().test(150.1)).isFalse();
    }

    @Test
    void return_unmodifiable_list() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor > 100.0 1");

        var monitors = manager.getMonitors();

        assertThatThrownBy(() -> monitors.add(new MonitorManager.MonitorData("> 200.0", d -> d > 200.0, 2)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void clear_all_monitors() {
        var manager = new MonitorManager();
        manager.processCommand("add monitor > 100.0 1");
        manager.processCommand("add monitor < 200.0 2");

        assertThat(manager.size()).isEqualTo(2);

        manager.clear();

        assertThat(manager.size()).isEqualTo(0);
        assertThat(manager.getMonitors()).isEmpty();
    }
}
