package ibhist;

import com.google.inject.Inject;

/**
 * Registers the domain event listeners while the container is being created.
 * Bound as an eager singleton in {@link AppModule}, so registration happens on startup
 * and App itself does no wiring.
 */
public class EventListenerRegistrar {

    @Inject
    public EventListenerRegistrar(EventService eventService, SoundPlayer soundPlayer) {
        eventService.listen(VolumeSpikeEvent.class, soundPlayer);
    }
}