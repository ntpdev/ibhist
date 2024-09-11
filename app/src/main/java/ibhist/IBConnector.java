package ibhist;

public interface IBConnector {

    enum ConnectorAction {
        ES_DAY,
        HISTORICAL,
        REALTIME
    }

    void process(ConnectorAction action);
}
