package ibhist;

public interface Action {
    int getRequestId();
    void makeRequest();
    void complete();

    void cancel();
}
