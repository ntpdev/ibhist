package ibhist;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.regex.Pattern;

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
        assertThat(StringColourise.colourise("the quick [red,0]brown[/] fox")).isEqualTo("the quick brown fox");
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
    void test_format() {
        assertThat(StringColourise.print("[yellow]Hello[/] [red,%d]%s[/]", 0, "World")).isEqualTo("\u001B[33mHello\u001B[0m World");
    }

    @Test
    void test_regex_match() {
        var text = "normal [green]green text[/] and [red,1]red text[/] normal";
        var matcher = StringColourise.matchColours.matcher(text);
        var xs = new ArrayList<String>();
        while (matcher.find()) {
            xs.add(matcher.group());
        }
        assertThat(xs).containsExactly("[green]","[/]","[red,1]","[/]");
    }
}

