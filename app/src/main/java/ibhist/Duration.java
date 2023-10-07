package ibhist;

public enum Duration {
    D1("1 D"),
    D5("5 D"),
    D10("10 D");

    private final String code;

    Duration(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
