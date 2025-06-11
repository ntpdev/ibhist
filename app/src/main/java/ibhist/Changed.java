package ibhist;

import java.time.Instant;

public class Changed<T> {
    private Instant timestamp;
    private T value;

    public Changed(T value) {
        timestamp = Instant.now();
        this.value = value;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public T getValue() {
        return value;
    }
}
