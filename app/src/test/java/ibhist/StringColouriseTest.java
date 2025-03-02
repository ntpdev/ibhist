package ibhist;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringColouriseTest {
    @Test
    void test_empty() {
        assertThat(StringColourise.colourise("")).isEqualTo("");
    }

    @Test
    void test_null() {
        assertThat(StringColourise.colourise(null)).isNull();
    }

    @Test
    void test_no_colours() {
        assertThat(StringColourise.colourise("the quick brown fox")).isEqualTo("the quick brown fox");
    }

    @Test
    void test_single_colour() {
        assertThat(StringColourise.colourise("Hello [red]world[/] it is monday")).isEqualTo("Hello \u001B[31mworld\u001B[0m it is monday");
    }

    @Test
    void test_multiple_colours() {
        assertThat(StringColourise.colourise("[green]Hello[/] [red]World[/]")).isEqualTo("\u001B[32mHello\u001B[0m \u001B[31mWorld\u001B[0m");
    }

    @Test
    void test_z() {
        StringColourise.z("normal [green]green text[/] and [red,1]red text[/] normal");
    }
}
