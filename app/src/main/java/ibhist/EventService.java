package ibhist;

/**
 * In-process synchronous event manager.
 * Listeners register by event type and are dispatched in registration order.
 */
public interface EventService {

    /**
     * Register a listener for a specific event type.
     *
     * @param type     the event class
     * @param listener the listener
     * @param <E>      the event type
     */
    <E> void listen(Class<E> type, EventListener<? super E> listener);

    /**
     * Publish an event to all registered listeners for its type, in registration order.
     * A listener returning {@code false} stops propagation.
     * A listener throwing is logged and dispatch continues to later listeners.
     * No listeners for the type is a no-op.
     *
     * @param event the event
     */
    <E> void publish(E event);
}