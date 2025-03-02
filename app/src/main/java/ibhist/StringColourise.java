package ibhist;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringColourise {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public static final Pattern matchColours = Pattern.compile("\\[(/?)(\\w+)(?:,(\\d+))?]");
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

    public static String colourise(String input) {
        var matcher = matchColours.matcher(input);
        var sb = new StringBuilder();
        boolean lastColorApplied = false;

        while (matcher.find()) {
            String replacement = "";
            String slash = matcher.group(1);
            String colorName = matcher.group(2);
            String flagStr = matcher.group(3);
            boolean flag = flagStr != null && flagStr.equals("1");

            if (!slash.isEmpty()) { // Handle reset tag [/]
                if (lastColorApplied) {
                    replacement = ANSI_RESET;
                    lastColorApplied = false;
                }
            } else { // Handle color tags
                if (flag) {
                    String ansiCode = getAnsiColour(colorName);
                    if (!ansiCode.isEmpty()) {
                        replacement = ansiCode;
                        lastColorApplied = true;
                    } else {
                        lastColorApplied = false;
                    }
                } else {
                    lastColorApplied = false;
                }
            }

            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
//    public static String colourise(String text) {
//        if (text == null || text.isEmpty()) {
//            return text;
//        }
//
//        var matcher = matchColours.matcher(text);
//        StringBuilder sb = new StringBuilder();
//        while (matcher.find()) {
//            matcher.appendReplacement(sb, getAnsiColour(matcher.group(1)) + matcher.group(2) + ANSI_RESET);
//        }
//        matcher.appendTail(sb);
//        return sb.toString();
//    }

    public static void z(String text) {
        var matcher = Pattern.compile("\\[\\w+(?:,\\d+)?]").matcher(text);

        while (matcher.find()) {
            System.out.println("Found: " + matcher.group());
        }
    }

    public static String print(String text) {
        String coloured = colourise(text);
        System.out.println(coloured);
        return coloured;
    }
}

