package ibhist;

public interface IBConnector extends ActionProvider {

    enum ConnectorAction {
        ES_DAY,
        HISTORICAL,
        REALTIME
    }

    void process(ConnectorAction action);

    boolean connect();

    void disconnect();

    HistoricalDataAction getHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate);

    HistoricalDataAction requestHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate);

    HistoricalDataAction waitForHistoricalData();

    RealTimeBarsAction requestRealTimeBars(String symbol, String contractMonth, MonitorManager manager);

    boolean cancelRealtime();
}
