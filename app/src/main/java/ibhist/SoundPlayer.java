package ibhist;

/**
 * Sound player that reacts to {@link VolumeSpikeEvent}s by playing a bundled WAV.
 * Holds a global in-memory enabled flag safely readable by background feed threads
 * while the REPL changes it.
 */
public interface SoundPlayer extends EventListener<VolumeSpikeEvent> {

    /**
     * Enable or disable playback. Does not persist across restarts.
     *
     * @param enabled true to enable, false to disable
     */
    void setEnabled(boolean enabled);

    /**
     * @return the current enabled state
     */
    boolean isEnabled();
}