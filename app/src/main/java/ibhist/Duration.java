package ibhist;

public enum Duration {
    DAY_1("1 D"),
    DAY_2("2 D"),
    DAY_5("5 D"),
    DAY_10("10 D"),
    YEAR_1("1 Y");

    private final String code;

    Duration(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
