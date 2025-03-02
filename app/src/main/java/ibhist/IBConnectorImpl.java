package ibhist;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;
import com.ib.client.EReader;
import com.ib.client.EReaderSignal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interface with IB
 * <a href="https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/">latest TWS docs</a>
 */
public class IBConnectorImpl implements IBConnector {
    private static final Logger log = LogManager.getLogger(IBConnectorImpl.class.getSimpleName());
    public static final String CONTRACT_MONTH = "202503";
    private EClientSocket m_client;
    private EReaderSignal m_signal;
    private EReader reader;
    private final Map<Integer, Action> actions = new TreeMap<>();
    private final AtomicInteger id = new AtomicInteger(100);
    private final BlockingQueue<Action> queue = new ArrayBlockingQueue<>(16);

    private final Provider<TimeSeriesRepository> timeSeriesRepository;
    private final ContractFactory contractFactory;


    @Inject
    public IBConnectorImpl(Provider<TimeSeriesRepository> timeSeriesRepository, ContractFactory contractFactory) {
        this.timeSeriesRepository = timeSeriesRepository;
        this.contractFactory = contractFactory;
    }

    @Override
    public void process(ConnectorAction action) {
        EWrapperImpl wrapper = new EWrapperImpl(actions);

        m_client = wrapper.getClient();
        m_signal = wrapper.getSignal();
        //! [connect]
        m_client.eConnect("127.0.0.1", 7496, 0);
        //! [connect]
        //! [ereader]
        reader = new EReader(m_client, m_signal);

        reader.start();
        new Thread(this::connectionThread).start();
        sleep(1_000);
        m_client.reqCurrentTime();
        switch (action) {
            case ES_DAY ->  getHistoricalData("ES", CONTRACT_MONTH, Duration.DAY_5, false);
            case HISTORICAL -> getMultipleHistoricalData(Duration.DAY_5);
            case REALTIME -> getRealTimeBars("ES", CONTRACT_MONTH);
        }
//        sleep(30_000);
        m_client.eDisconnect();
        sleep(1_000);
    }

    private void getMultipleHistoricalData(Duration duration) {
        getHistoricalIndexData("TICK-NYSE", "NYSE", duration, false);
        getHistoricalData("ES", CONTRACT_MONTH, duration, false);
        getHistoricalData("NQ", CONTRACT_MONTH, duration, false);
    }

    private <T> T takeFromQueue(Class<T> clz) throws InterruptedException {
        return clz.cast(queue.take());
    }

    void connectionThread() {
        try {
            log.info("starting waiting for connection");
            while (m_client.isConnected()) {
                m_signal.waitForSignal();
//            log.info("connected");
                reader.processMsgs();
            }
            log.info("ended");
        } catch (Exception e) {
            log.error(e);
        }
    }


    private void getHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate) {
        try {
            requestContractDetails(symbol, contractMonth);
            var action = takeFromQueue(ContractDetailsAction.class);
            processContractDetails(action);

            requestHistoricalData(action.getContract(), duration, keepUpToDate);
            var hdAction = takeFromQueue(HistoricalDataAction.class);
            processHistoricalData(hdAction, true);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    private void getHistoricalIndexData(String symbol, String exchange, Duration duration, boolean keepUpToDate) {
        try {
            var contract = contractFactory.newIndex(symbol, exchange);
            requestHistoricalData(contract, duration, keepUpToDate);
            var hdAction = takeFromQueue(HistoricalDataAction.class);
            processHistoricalIndexData(hdAction);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    private void getRealTimeBars(String symbol, String contractMonth) {
        try {
            requestContractDetails(symbol, contractMonth);
            var action = takeFromQueue(ContractDetailsAction.class);
            processContractDetails(action);

            requestRealTimeBars(action.getContract());
            var rtAction = takeFromQueue(RealTimeBarsAction.class);
            processRealTimeBars(rtAction);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    private void requestRealTimeBars(Contract contract) {
        log.info("requestRealTimeBars");
        var action = new RealTimeBarsAction(m_client, id, queue, contract);
        actions.put(action.getRequestId(), action);
        action.makeRequest();
    }

    private void processRealTimeBars(RealTimeBarsAction rtAction) {
        log.info("processRealTimeBars");
    }

    private void requestContractDetails(String symbol, String contractMonth) {
        log.info("contractOperations");
        var action = new ContractDetailsAction(m_client, id, queue, contractFactory.newFutureContract(symbol, contractMonth));
        actions.put(action.getRequestId(), action);
        action.makeRequest();
    }

    private void processContractDetails(ContractDetailsAction action) {
        var cd = action.getContractDetails();
        log.info(cd.conid() + " " + cd.contract().description());
    }

    private void requestHistoricalData(Contract contract, Duration duration, boolean keepUpToDate) {
        log.info("historicalData");
        var action = new HistoricalDataAction(m_client, id, queue, contract, duration, keepUpToDate);
        actions.put(action.getRequestId(), action);
        action.makeRequest();
    }

    private void processHistoricalData(HistoricalDataAction action, boolean fSave) {
        var bars = action.getBars();
        var contract = action.getContract();
        log.info("processHistoricalData " + bars.size());
        if (!bars.isEmpty()) {
            log.info(contract.localSymbol() + " bars from " + bars.getFirst().time() + " to " + bars.getLast().time());
            var history = action.getPriceHistory();
            var vwap = history.vwap("vwap");
            var rthBars = history.rthBars();
            for (var bar : rthBars) {
                log.info(bar.asDailyBar());
            }
            var index = history.index();
            var entry = index.entries().getLast();

            log.info(index.logIntradayInfo(index.makeMessages(entry)));
            if (fSave) {
                action.save(index.entries().getFirst().tradeDate());
            }
            // update repository
            timeSeriesRepository.get().append(history);
        }
    }

    private void processHistoricalIndexData(HistoricalDataAction action) {
        var bars = action.getBars();
        var contract = action.getContract();
        log.info("processHistoricalData " + bars.size());
        if (!bars.isEmpty()) {
            log.info(contract.symbol() + " bars from " + bars.getFirst().time() + " to " + bars.getLast().time());
            var history = action.getPriceHistory();
            action.save(history.getDates()[0].toLocalDate());
        }
    }

    static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {}
    }
}
