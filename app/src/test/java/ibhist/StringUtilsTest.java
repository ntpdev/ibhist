package ibhist;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class StringUtilsTest {
    @Test
    void colourise_empty() {
        assertThat(StringUtils.colourise("")).isEqualTo("");
    }

    @Test
    void colourise_null() {
        assertThat(StringUtils.colourise(null)).isNull();
    }

    @Test
    void colourise_no_colours() {
        assertThat(StringUtils.colourise("the quick red fox")).isEqualTo("the quick red fox");
        assertThat(StringUtils.colourise("the quick [red,0]brown[/] fox")).isEqualTo("the quick brown fox");
    }

    @Test
    void colourise_single_colour() {
        assertThat(StringUtils.colourise("Hello [red]world[/] it is monday")).isEqualTo("Hello \u001B[31mworld\u001B[0m it is monday");
        assertThat(StringUtils.colourise("Hello [red,1]world[/] it is monday")).isEqualTo("Hello \u001B[31mworld\u001B[0m it is monday");
    }

    @Test
    void colourise_multiple_colours() {
        assertThat(StringUtils.colourise("[green]Hello[/] [red]World[/]")).isEqualTo("\u001B[32mHello\u001B[0m \u001B[31mWorld\u001B[0m");
    }

    @Test
    void print_null() {
        String s = null;
        assertThat(StringUtils.print(s)).isEqualTo("null");
    }

    @Test
    void print_formatting() {
        var sb = new StringBuilder();
        sb.append("the number ").append(42);
        assertThat(StringUtils.print(sb)).isEqualTo("the number 42");

        assertThat(StringUtils.print("[yellow]high[/] [red,%d]%.2f[/]", 0, 123.456)).isEqualTo("\u001B[33mhigh\u001B[0m 123.46");
        assertThat(StringUtils.print("[yellow]low[/] [green,%d]%.2f[/]", 1, 123.456)).isEqualTo("\u001B[33mlow\u001B[0m \u001B[32m123.46\u001B[0m");
    }

    @Test
    void match_colours_regex_match() {
        var text = "normal [green]green text[/] and [red,1]red text[/] normal";
        var matcher = StringUtils.matchColours.matcher(text);
        var xs = new ArrayList<String>();
        while (matcher.find()) {
            xs.add(matcher.group());
        }
        assertThat(xs).containsExactly("[green]","[/]","[red,1]","[/]");
    }

    @Test
    void split_null_input_gives_empty_list() {
        assertThat(StringUtils.split(null)).isEmpty();
    }

    @Test
    void split_empty_input_gives_empty_list() {
        assertThat(StringUtils.split("")).isEmpty();
        assertThat(StringUtils.split("   ")).isEmpty();
    }

    @Test
    void split_simple_words() {
        var result = StringUtils.split("hello side note world");
        assertThat(result).containsExactly("hello", "side", "note", "world");
    }

    @Test
    void split_double_quoted_phrase() {
        var result = StringUtils.split("hello \"side note\" world");
        assertThat(result).containsExactly("hello", "side note", "world");
    }

    @Test
    void split_unterminated_double_quote() {
        var result = StringUtils.split("hello \"side note world");
        assertThat(result).containsExactly("hello", "side note world");
    }

    @Test
    void split_single_quoted_with_inner_double() {
        var result = StringUtils.split("hello 'side note \"fred\"' world");
        assertThat(result).containsExactly("hello", "side note \"fred\"", "world");
    }

    @Test
    void split_mixed_quotes_and_words() {
        String in = "foo 'bar baz' qux \"quux corge\" grault 'garply";
        var result = StringUtils.split(in);
        assertThat(result).containsExactly(
                "foo",
                "bar baz",
                "qux",
                "quux corge",
                "grault",
                "garply" // unterminated single-quoted at end
        );
    }
}
