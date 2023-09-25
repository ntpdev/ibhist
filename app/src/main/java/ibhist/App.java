/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package ibhist;

import com.ib.client.Contract;
import com.ib.client.EClientSocket;
import com.ib.client.EReader;
import com.ib.client.EReaderSignal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class App {
    private static final Logger log = LogManager.getLogger("App");
    private static EClientSocket m_client;
    private static EReaderSignal m_signal;
    private static EReader reader;
    private static final Map<Integer, Action> actions = new TreeMap<>();

    private static Contract contract;

    private static final AtomicInteger id = new AtomicInteger(100);

    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        log.info(System.getProperty("java.version"));
        EWrapperImpl wrapper = new EWrapperImpl(actions);

        m_client = wrapper.getClient();
        m_signal = wrapper.getSignal();
        //! [connect]
        m_client.eConnect("127.0.0.1", 7496, 0);
        //! [connect]
        //! [ereader]
        reader = new EReader(m_client, m_signal);

        reader.start();
        new Thread(App::connectionThread).start();
        sleep(1_000);
        contractOperations();
        sleep(10_000);
        m_client.eDisconnect();
    }

    public static void connectionThread() {
        log.info("starting waiting for connection");
        while (m_client.isConnected()) {
            m_signal.waitForSignal();
//            log.info("connected");
            try {
                reader.processMsgs();
            } catch (Exception e) {
                System.out.println("Exception: " + e.getMessage());
            }
        }
        log.info("ended");
    }

    private static void contractOperations() {
        log.info("contractOperations");
        m_client.reqCurrentTime();
        sleep(1_000);
//        m_client.reqContractDetails(id.getAndIncrement(), simpleFuture("ES", "202312"));
        var action = new ContractDetailsAction(m_client, id, makeFuture("ES", "202312"), App::processContractDetails);
        actions.put(action.getRequestId(), action);
        action.request();
    }

    private static void historicalData(){
        log.info("historicalData");
        var action = new HistoricalDataAction(m_client, id, contract, App::processHistoricalData);
        actions.put(action.getRequestId(), action);
        action.request();
        sleep(5_000);
        action.cancel();
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {}
    }

    private static void processContractDetails(ContractDetailsAction action) {
        var cd = action.getContractDetails();
        log.info(cd.conid());
        contract = action.getContract();
        historicalData();
    }

    private static void processHistoricalData(HistoricalDataAction action) {
        var bars = action.getBars();
        var contract = action.getContract();
        log.info("processHistoricalData " + bars.size());
        if (!bars.isEmpty()) {
            log.info(contract.localSymbol() + " bars from " + bars.get(0).time() + " to " + bars.get(bars.size() - 1).time());
            var history = action.getPriceHistory();
            var index = history.makeIndex();
//            action.save();
        }
    }

    public static Contract makeFuture(String symbol, String contractMonth) {
        //! [futcontract]
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.lastTradeDateOrContractMonth(contractMonth);
        contract.secType("FUT");
        contract.currency("USD");
        contract.exchange("CME");
        //! [futcontract]
        return contract;
    }
}
