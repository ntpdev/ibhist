package ibhist;

public interface IBConnector {

    enum ConnectorAction {
        ES_DAY,
        HISTORICAL,
        REALTIME
    }

    void process(ConnectorAction action);
    void connect();
    void disconnect();
    void requestRealTimeBars(String symbol, String contractMonth, MonitorManager manager);
    boolean cancelRealtime();
}
