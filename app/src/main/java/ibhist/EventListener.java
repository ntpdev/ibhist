package ibhist;

/**
 * Functional listener for domain events.
 * Returns {@code true} to continue propagation to later listeners, {@code false} to stop.
 */
@FunctionalInterface
public interface EventListener<E> {
    boolean onEvent(E event);
}