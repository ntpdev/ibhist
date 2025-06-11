package ibhist;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ib.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static ibhist.StringUtils.print;

/**
 * Interface with IB
 * <a href="https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/">latest TWS docs</a>
 */
public class IBConnectorImpl implements IBConnector, ActionProvider {
    private static final Logger log = LogManager.getLogger(IBConnectorImpl.class.getSimpleName());
    public static final String CONTRACT_MONTH = "202506";
    private EClientSocket m_client;
    private EReaderSignal m_signal;
    private EReader reader;
    private final AtomicInteger id = new AtomicInteger(100); // unique id used for Action
    private final AtomicInteger nextOrderId = new AtomicInteger(0);
    private final Map<ContractKey, ContractDetails> contractCache = new HashMap<>();
    private final Map<Integer, Action> actions = new TreeMap<>(); // holds outstanding async requests
    private final BlockingQueue<Action> queue = new ArrayBlockingQueue<>(16); // holds completed actions sent from worker thread

    private final Provider<TimeSeriesRepository> timeSeriesRepository;
    private final ContractFactory contractFactory;


    @Inject
    public IBConnectorImpl(Provider<TimeSeriesRepository> timeSeriesRepository, ContractFactory contractFactory) {
        this.timeSeriesRepository = timeSeriesRepository;
        this.contractFactory = contractFactory;
    }

    @Override
    public void process(ConnectorAction action) {
        log.info("process action = " + action);
        if (connect()) {
            try {
                switch (action) {
                    case ES_DAY -> saveHistoricalData("ES", CONTRACT_MONTH, Duration.DAY_2);
                    case HISTORICAL -> saveMultipleHistoricalData(Duration.DAY_5);
                    case REALTIME -> requestRealTimeBars("ES", CONTRACT_MONTH, null); // never called
                }
            } finally{
                disconnect();
            }
        }
        log.info("process end");
    }

    @Override
    public boolean connect() {
        EWrapperImpl wrapper = new EWrapperImpl(this);
        m_client = wrapper.getClient();
        m_signal = wrapper.getSignal();

        m_client.eConnect("127.0.0.1", 7496, 0);
        reader = new EReader(m_client, m_signal);

        var nextOrder = new NextOrderIdAction(m_client, id, queue);
        actions.put(nextOrder.getRequestId(), nextOrder); // this is a dummy action that does not need to be sent

        var orderManager = new OrderManagerAction(m_client, id, queue);
        actions.put(orderManager.getRequestId(), orderManager); // this is a dummy action that does not need to be sent)

        reader.start();
        new Thread(this::connectionThread).start();

        if (!m_client.isConnected()) {
            return false;
        }

        var orderIdAction = takeFromQueue(NextOrderIdAction.class);
        nextOrderId.set(orderIdAction.getOrderId());

        m_client.reqCurrentTime();
        return true;
    }

    @Override
    public void disconnect() {
        if (!actions.isEmpty()) {
            log.warn("WARNING: actions remain in map " + actionsToString());
            try {
                var action = queue.poll(5_000, TimeUnit.MILLISECONDS);
                if (action != null) {
                    log.info(action);
                    actions.remove(action.getRequestId());
                    log.info("actions map " + actions.size());
                }
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
        m_client.eDisconnect();
        sleep(1_000);
    }

    Action sendRequest(Action action) {
        actions.put(action.getRequestId(), action);
        action.makeRequest();
        return action;
    }

    @Override
    public boolean cancelRealtime() {
        var action = findByType(RealTimeBarsAction.class);
        action.ifPresent(RealTimeBarsAction::forceCancel);
        return action.isPresent();
    }

    private void saveMultipleHistoricalData(Duration duration) {
        saveHistoricalData("ES", CONTRACT_MONTH, duration);
        saveHistoricalData("NQ", CONTRACT_MONTH, duration);
        getHistoricalIndexData("TICK-NYSE", duration, false);
    }

    /**
     * blocking call waits for an item in the queue and casts to required type
     * the action is removed from the actions map
     */
    private <T extends Action> T takeFromQueue(Class<T> clz) {
        try {
            var a = queue.take();
            actions.remove(a.getRequestId());
            return clz.cast(a);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while taking from queue", e);
            throw new RuntimeException("Interrupted while taking from queue", e);
        }
    }

//    @SuppressWarnings("unchecked");
    private <T extends Action> T takeFromQueue(T action) {
        Class<T> c = (Class<T>)action.getClass();
        return takeFromQueue(c);
    }

    void connectionThread() {
        try {
            log.info("connectionThread: started");
            while (m_client.isConnected()) {
                m_signal.waitForSignal();
                reader.processMsgs();
            }
            log.info("connectionThread: ended");
        } catch (Exception e) {
            log.error(e);
        }
    }

    @Override
    public HistoricalDataAction requestHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate) {
        var contractDetails = getContractDetails(contractFactory.newFutureContract(symbol, contractMonth));
        return requestHistoricalData(contractDetails.contract(), duration, keepUpToDate);
    }

    @Override
    public HistoricalDataAction waitForHistoricalData() {

        return takeFromQueue(HistoricalDataAction.class);
    }

    /**
     * requests historical data. returns as soon as historic data is fetched.
     * if keepUpToDate = true, then the HDAction will continue to be updated until its time limit is reached
     */
    @Override
    public HistoricalDataAction getHistoricalData(String symbol, String contractMonth, Duration duration, boolean keepUpToDate) {
        var contractDetails = getContractDetails(contractFactory.newFutureContract(symbol, contractMonth));
        var sent = requestHistoricalData(contractDetails.contract(), duration, keepUpToDate);
        return takeFromQueue(sent);
    }

    public void saveHistoricalData(String symbol, String contractMonth, Duration duration) {
        var hdAction = getHistoricalData(symbol, contractMonth, duration, false);
        processHistoricalData(hdAction, true);
    }

    private void getHistoricalIndexData(String symbol, Duration duration, boolean keepUpToDate) {
        var contract = contractFactory.newIndex(symbol);
        var sent = requestHistoricalData(contract, duration, keepUpToDate);
        processHistoricalIndexData(takeFromQueue(sent));
    }

    /**
     * non blocking call - requests streaming data
     */
    @Override
    public RealTimeBarsAction requestRealTimeBars(String symbol, String contractMonth, MonitorManager manager) {
        var contractDetails = getContractDetails(contractFactory.newFutureContract(symbol, contractMonth));
        return requestRealTimeBars(contractDetails.contract(), manager);
    }

    private RealTimeBarsAction requestRealTimeBars(Contract contract, MonitorManager manager) {
        log.info("requestRealTimeBars");
        var action = new RealTimeBarsAction(m_client, id, queue, contract, manager, 120);
        sendRequest(action);
        return action;
    }

    private void processRealTimeBars(RealTimeBarsAction rtAction) {
        log.info("processRealTimeBars");
    }

    private ContractDetailsAction requestContractDetails(Contract contract) {
        var action = new ContractDetailsAction(m_client, id, queue, contract);
        sendRequest(action);
        return action;
    }

//    private void processContractDetails(ContractDetailsAction action) {
//        var cd = action.getContractDetails();
//        log.info(cd.conid() + " " + cd.contract().description());
//    }

    public ContractDetails getContractDetails(Contract contract) {
        var key = new ContractKey(contract.symbol(), contract.lastTradeDateOrContractMonth(), contract.exchange());
        return contractCache.computeIfAbsent(key, k -> {
            requestContractDetails(contract);
            var action = takeFromQueue(ContractDetailsAction.class);
            return action.getContractDetails();
        });
    }

    private HistoricalDataAction requestHistoricalData(Contract contract, Duration duration, boolean keepUpToDate) {
        var action = new HistoricalDataAction(m_client, id, queue, contract, duration, keepUpToDate);
        sendRequest(action);
        return action;
    }

    private Path processHistoricalData(HistoricalDataAction action, boolean fSave) {
        var bars = action.getBars();
        var contract = action.getContract();
        Path path = null;
        log.info("processHistoricalData {} {}", bars.size(), contract.localSymbol());
        if (!bars.isEmpty()) {
            log.info("bars from {} to {}", bars.getFirst().time(), bars.getLast().time());

            var history = action.asPriceHistory();
            var rthBars = history.rthBars();
            var sb = new StringBuilder();
            sb.append("\n[yellow]--- RTH bars ---\n");
            for (var bar : rthBars) {
                sb.append(bar.asDailyBar()).append("\n");
            }
            sb.append("[/]");
            print(sb);

            print(history.intradayPriceInfo(-1));

            if (fSave) {
                var index = history.index();
                path = action.save(index.entries().getFirst().tradeDate());
                // update repository
                timeSeriesRepository.get().append(history);
            }
        }
        return path;
    }

    private void processHistoricalIndexData(HistoricalDataAction action) {
        var bars = action.getBars();
        var contract = action.getContract();
        log.info("processHistoricalIndexData " + bars.size());
        if (!bars.isEmpty()) {
            log.info(contract.symbol() + " bars from " + bars.getFirst().time() + " to " + bars.getLast().time());
            var history = action.asPriceHistory();
            action.save(history.getDates()[0].toLocalDate());
        }
    }

    static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {}
    }

    @Override
    public <T extends Action> Optional<T> findByType(Class<T> clz) {
        for (Action action : actions.values()) {
            if (clz.isInstance(action)) {
                return Optional.of(clz.cast(action));
            }
        }
        return Optional.empty();
    }

    @Override
    public Action findById(int id) {
        return actions.get(id);
    }

    @Override
    public String actionsToString() {
        var sb = new StringBuilder();
        sb.append("[yellow]Action map size = " + actions.size());
        for (Action value : actions.values()) {
            sb.append("\n").append(value);
        }
        sb.append("[/]\n");
        return sb.toString();
    }


    public record ContractKey(
            String symbol,
            String contractMonth,  // only non‐null for futures
            String exchange        // only non‐null for indexes
    ) {}
}
