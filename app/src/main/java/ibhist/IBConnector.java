package ibhist;

import java.util.List;

public interface IBConnector extends ActionProvider {

    enum ConnectorAction {
        ES_DAY,
        LATEST_WEEK,
        HISTORICAL,
        REALTIME
    }

    void process(ConnectorAction action);

    boolean connect();

    void disconnect();

    HistoricalDataAction getHistoricalData(String symbol, String contractMonth, Duration duration);

    HistoricalDataAction requestHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate, MonitorManager manager);

    HistoricalDataAction waitForHistoricalData();

    RealTimeBarsAction requestRealTimeBars(String symbol, String contractMonth, MonitorManager manager);

    boolean cancelRealtime();

    void placeOrders(String symbol, List<OrderDetails> orderDetails);

    void buildOrder(String symbol);
}
