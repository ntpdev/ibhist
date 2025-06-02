package ibhist;

public interface IBConnector {

    enum ConnectorAction {
        ES_DAY,
        HISTORICAL,
        REALTIME
    }

    void process(ConnectorAction action);
    boolean connect();
    void disconnect();
    HistoricalDataAction getHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate);
    RealTimeBarsAction requestRealTimeBars(String symbol, String contractMonth, MonitorManager manager);
    boolean cancelRealtime();
}
