package ibhist;

import java.time.LocalDateTime;

/**
 * Domain event signalling that a volume spike was detected.
 * Initial payload is the timestamp only.
 */
public record VolumeSpikeEvent(LocalDateTime timestamp) {
}