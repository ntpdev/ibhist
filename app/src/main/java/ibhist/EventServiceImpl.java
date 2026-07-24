package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Synchronous in-process event manager. Maintains listeners by event type and
 * dispatches in registration order. Listener exceptions are logged and dispatch
 * continues to later listeners. No listeners for a type is a valid no-op.
 */
public class EventServiceImpl implements EventService {

    private static final Logger log = LogManager.getLogger(EventServiceImpl.class.getSimpleName());

    private final Map<Class<?>, List<EventListener<?>>> listeners = new ConcurrentHashMap<>();

    @Override
    public <E> void listen(Class<E> type, EventListener<? super E> listener) {
        listeners.computeIfAbsent(type, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> void publish(E event) {
        var list = listeners.get(event.getClass());
        if (list == null || list.isEmpty()) {
            return;
        }
        for (var listener : list) {
            try {
                boolean propagate = ((EventListener<E>) listener).onEvent(event);
                if (!propagate) {
                    break;
                }
            } catch (Exception e) {
                log.error("event listener {} threw for {}", listener, event, e);
            }
        }
    }
}