package ibhist;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class ReplImplTest {

    @Test
    void sample() {
        long now = Instant.now().getEpochSecond();
        // only encode last 6 bytes to avoid padding
        byte[] bytes = new byte[6];
        for (int i = 5; i >= 0; i--) {
            bytes[i] = (byte)(now & 0xff);
            now >>= 8;
        }
        String s = Base64.getEncoder().encodeToString(bytes);
        String end = s.substring(s.length() - 4);
        assertThat(end).isEqualTo("W6Vu");
    }
}