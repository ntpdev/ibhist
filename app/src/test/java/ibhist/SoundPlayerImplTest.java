package ibhist;

import org.junit.jupiter.api.Test;

import javax.sound.sampled.Clip;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class SoundPlayerImplTest {

    @Test
    void enabled_round_trip() {
        var player = new SoundPlayerImpl(mock(Clip.class));

        assertThat(player.isEnabled()).isTrue();

        player.setEnabled(false);
        assertThat(player.isEnabled()).isFalse();

        player.setEnabled(true);
        assertThat(player.isEnabled()).isTrue();
    }

    @Test
    void onEvent_when_disabled_skips_playback_and_returns_true() {
        var clip = mock(Clip.class);
        var player = new SoundPlayerImpl(clip);
        player.setEnabled(false);

        boolean result = player.onEvent(new VolumeSpikeEvent(java.time.LocalDateTime.now()));

        assertThat(result).isTrue();
        verifyNoMoreInteractions(clip);
    }

    @Test
    void onEvent_when_enabled_resets_and_starts_clip_and_returns_true() {
        var clip = mock(Clip.class);
        var player = new SoundPlayerImpl(clip);

        boolean result = player.onEvent(new VolumeSpikeEvent(java.time.LocalDateTime.now()));

        assertThat(result).isTrue();
        verify(clip).setFramePosition(0);
        verify(clip).start();
    }

    @Test
    void onEvent_with_null_clip_noops_and_returns_true() {
        var player = new SoundPlayerImpl((Clip) null);

        boolean result = player.onEvent(new VolumeSpikeEvent(java.time.LocalDateTime.now()));

        assertThat(result).isTrue();
    }

    @Test
    void onEvent_isolates_clip_failure_and_returns_true() {
        var clip = mock(Clip.class);
        org.mockito.Mockito.doThrow(new RuntimeException("audio fail")).when(clip).start();
        var player = new SoundPlayerImpl(clip);

        boolean result = player.onEvent(new VolumeSpikeEvent(java.time.LocalDateTime.now()));

        assertThat(result).isTrue();
        verify(clip).setFramePosition(0);
        verify(clip).start();
    }
}