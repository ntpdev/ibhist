package ibhist;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventServiceImplTest {

    private EventService eventService;

    @BeforeEach
    void setUp() {
        eventService = new EventServiceImpl();
    }

    @Test
    void publish_with_no_listeners_is_noop() {
        eventService.publish(new VolumeSpikeEvent(java.time.LocalDateTime.now()));
    }

    @Test
    void dispatches_in_registration_order() {
        List<String> calls = new ArrayList<>();
        eventService.listen(String.class, e -> { calls.add("a:" + e); return true; });
        eventService.listen(String.class, e -> { calls.add("b:" + e); return true; });

        eventService.publish("hi");

        assertThat(calls).containsExactly("a:hi", "b:hi");
    }

    @Test
    void false_stops_propagation() {
        List<String> calls = new ArrayList<>();
        eventService.listen(String.class, e -> { calls.add("a"); return false; });
        eventService.listen(String.class, e -> { calls.add("b"); return true; });

        eventService.publish("x");

        assertThat(calls).containsExactly("a");
    }

    @Test
    void true_continues_to_later_listeners() {
        List<String> calls = new ArrayList<>();
        eventService.listen(String.class, e -> { calls.add("a"); return true; });
        eventService.listen(String.class, e -> { calls.add("b"); return true; });

        eventService.publish("x");

        assertThat(calls).containsExactly("a", "b");
    }

    @Test
    void listener_exception_is_isolated_and_dispatch_continues() {
        List<String> calls = new ArrayList<>();
        eventService.listen(String.class, e -> { calls.add("a"); throw new RuntimeException("boom"); });
        eventService.listen(String.class, e -> { calls.add("b"); return true; });

        eventService.publish("x");

        assertThat(calls).containsExactly("a", "b");
    }

    @Test
    void listener_types_are_isolated() {
        List<String> calls = new ArrayList<>();
        eventService.listen(String.class, e -> { calls.add("str"); return true; });
        eventService.listen(Integer.class, e -> { calls.add("int"); return true; });

        eventService.publish(42);

        assertThat(calls).containsExactly("int");
    }
}