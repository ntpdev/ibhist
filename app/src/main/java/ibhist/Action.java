package ibhist;

public interface Action {
    int getRequestId();
    void request();
    void process();

    void cancel();
}
