package ibhist;

public interface Action {
    int getRequestId();
    void makeRequest();
    void process();

    void cancel();
}
