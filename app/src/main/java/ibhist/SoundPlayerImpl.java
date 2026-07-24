package ibhist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;

/**
 * Plays a bundled WAV resource when a {@link VolumeSpikeEvent} is published.
 * Playback is non-blocking: {@link Clip#start()} returns immediately and audio
 * plays on the mixer thread. Audio failures are logged and do not affect feed
 * processing.
 */
public class SoundPlayerImpl implements SoundPlayer {

    private static final Logger log = LogManager.getLogger(SoundPlayerImpl.class.getSimpleName());

    private static final String RESOURCE_PATH = "/notification.wav";

    private final Clip clip;
    private volatile boolean enabled = true;

    /**
     * Production constructor: preloads the bundled {@code notification.wav} resource.
     * Construction failures are swallowed and logged; the instance is left in a
     * safe no-op state.
     */
    public SoundPlayerImpl() {
        this(loadClip(RESOURCE_PATH));
    }

    /**
     * Test/dependency-injection constructor allowing a preconfigured {@link Clip} instance.
     *
     * @param clip a clip to play, or {@code null} if initialisation failed
     */
    SoundPlayerImpl(Clip clip) {
        this.clip = clip;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public boolean onEvent(VolumeSpikeEvent event) {
        if (!enabled || clip == null) {
            return true;
        }
        try {
            clip.setFramePosition(0);
            clip.start();
        } catch (Exception e) {
            log.error("failed to play notification sound", e);
        }
        return true;
    }

    private static Clip loadClip(String resourcePath) {
        try (var in = SoundPlayerImpl.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                log.error("bundled sound resource not found on classpath: {}", resourcePath);
                return null;
            }
            var bytes = in.readAllBytes();
            try (AudioInputStream ais = AudioSystem.getAudioInputStream(new java.io.ByteArrayInputStream(bytes))) {
                Clip loaded = AudioSystem.getClip();
                loaded.open(ais);
                return loaded;
            }
        } catch (Exception e) {
            log.error("failed to preload bundled sound resource: {}", resourcePath, e);
            return null;
        }
    }
}