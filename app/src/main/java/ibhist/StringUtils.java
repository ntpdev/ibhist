package ibhist;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Yet another StringUtils class
 */
public class StringUtils {
    public static final Splitter WS_SPLITTER = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings();
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

//    public static final Pattern matchColours = Pattern.compile("\\[(/?)(\\w+)(?:,(\\d+))?]");
    // groups: 1 = optional "/" (closing), 2 = primary colour, 3 = secondary colour OR legacy flag,
    //         4 = flag when secondary colour present
    public static final Pattern matchColours = Pattern.compile("\\[(/?)(\\w*)(?:,(\\w+)(?:,(\\d+))?)?]");
//            Pattern.compile("\\[(black|red|green|yellow|blue|purple|cyan|white)](.*?)\\[/]");

    public static String getAnsiColour(String colour) {
        return switch (colour.toLowerCase()) {
            case "black" -> ANSI_BLACK;
            case "red" -> ANSI_RED;
            case "green" -> ANSI_GREEN;
            case "yellow" -> ANSI_YELLOW;
            case "blue" -> ANSI_BLUE;
            case "purple" -> ANSI_PURPLE;
            case "cyan" -> ANSI_CYAN;
            case "white" -> ANSI_WHITE;
            default -> ANSI_RESET;
        };
    }

    /**
     * Replace markup colours by ANSI codes. Colours can be conditional.
     * Supported forms:
     * <ul>
     *   <li>{@code [red]text[/]} - unconditional colour</li>
     *   <li>{@code [red,1]text[/]} - legacy single-colour conditional; coloured when flag is 1</li>
     *   <li>{@code [red,0]text[/]} - legacy single-colour conditional; plain when flag is 0</li>
     *   <li>{@code [green,red]text[/]} - two-colour conditional; primary when flag (default 1)</li>
     *   <li>{@code [green,red,1]text[/]} - primary colour when flag is 1</li>
     *   <li>{@code [green,red,0]text[/]} - secondary colour when flag is 0</li>
     * </ul>
     * @param input - markup string
     * @return ANSI-coloured string
     */
    public static String colourise(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        var matcher = matchColours.matcher(input);
        var sb = new StringBuilder();
        boolean colorActive = false;

        while (matcher.find()) {
            boolean isClosingTag = !matcher.group(1).isEmpty();
            String colorName = matcher.group(2);

            if (isClosingTag) {
                // Only apply reset if a color was actually applied
                if (colorActive) {
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(ANSI_RESET));
                    colorActive = false;
                } else {
                    // Skip this closing tag since no color was applied
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(""));
                }
                continue;
            }

            // Handle opening tags
            if (colorName.isEmpty()) {
                // Empty color name in opening tag - skip it
                matcher.appendReplacement(sb, Matcher.quoteReplacement(""));
                continue;
            }

            String secondGroup = matcher.group(3);
            String flagGroup = matcher.group(4);

            if (secondGroup == null) {
                // No secondary info: [color]text[/] - apply primary unconditionally
                String ansiCode = getAnsiColour(colorName);
                if (!ansiCode.isEmpty()) {
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(ansiCode));
                    colorActive = true;
                } else {
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(""));
                }
                continue;
            }

            int flagValue;
            boolean isTwoColorForm;

            try {
                flagValue = Integer.parseInt(secondGroup);
                isTwoColorForm = false;
            } catch (NumberFormatException e) {
                isTwoColorForm = true;
                flagValue = (flagGroup != null) ? Integer.parseInt(flagGroup) : 1;
            }

            if (isTwoColorForm) {
                // [primary,secondary[,flag]] - always coloured; pick colour by flag
                String ansiCode = getAnsiColour(colorName);
                if (flagValue != 1) {
                    ansiCode = getAnsiColour(secondGroup);
                    if (ansiCode.equals(ANSI_RESET)) {
                        ansiCode = getAnsiColour(colorName);
                    }
                }
                if (ansiCode.isEmpty() || ansiCode.equals(ANSI_RESET)) {
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(""));
                    continue;
                }
                matcher.appendReplacement(sb, Matcher.quoteReplacement(ansiCode));
                colorActive = true;
                continue;
            }

            // Legacy single-colour conditional [color,flag]
            if (flagValue == 1) {
                String ansiCode = getAnsiColour(colorName);
                if (!ansiCode.isEmpty() && !ansiCode.equals(ANSI_RESET)) {
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(ansiCode));
                    colorActive = true;
                    continue;
                }
            }

            // flag 0 or unknown colour: emit plain text
            matcher.appendReplacement(sb, Matcher.quoteReplacement(""));
        }

        matcher.appendTail(sb);
        return sb.toString();
    }

    public static String print(StringBuilder text) {
        return StringUtils.print(text.toString());
    }

    public static String print(String text) {
        if (text == null || text.isEmpty())  {
            text = "null";
        }
        String coloured = colourise(text);
        System.out.println(coloured);
        return coloured;
    }

    public static String print(String format, Object... args) {
        String coloured = colourise(format.formatted(args));
        System.out.println(coloured);
        return coloured;
    }


    /**
     * Splits the input on whitespace, then recombines tokens
     * that begin with a single or double quote into one phrase
     * (including any internal whitespace). Unterminated quotes
     * are assumed to close at end of input.
     *
     * @param input the raw string
     * @return list of tokens / phrases, never null
     */
    public static List<String> split(String input) {
        if (input == null) {
            return Collections.emptyList();
        }
        List<String> tokens = WS_SPLITTER.splitToList(input);
        List<String> result = new ArrayList<>(tokens.size());

        StringBuilder buffer = null;
        char quoteChar = 0;

        for (String token : tokens) {
            if (buffer == null) {
                // Not in the middle of a quoted phrase
                boolean isQuote = token.charAt(0) == '"' || token.charAt(0) == '\'';
                if (token.length() > 1
                        && isQuote
                        && token.charAt(token.length() - 1) == token.charAt(0)) {
                    // Fully quoted in one token: "foo" or 'bar'
                    result.add(token.substring(1, token.length() - 1));
                } else if (token.length() >= 1 && isQuote) {
                    // Start of a quoted phrase
                    quoteChar = token.charAt(0);
                    buffer = new StringBuilder(token.substring(1));
                } else {
                    // Ordinary token
                    result.add(token);
                }
            } else {
                // We're inside a quoted phrase
                buffer.append(' ').append(token);
                if (token.charAt(token.length() - 1) == quoteChar) {
                    // End of quoted phrase
                    // Drop the trailing quote
                    buffer.setLength(buffer.length() - 1);
                    result.add(buffer.toString());
                    buffer = null;
                    quoteChar = 0;
                }
            }
        }

        // Unterminated quote: flush buffer as-is
        if (buffer != null) {
            result.add(buffer.toString());
        }

        return result;
    }
}

