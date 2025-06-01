package ibhist;

import com.ib.client.*;
import com.ib.client.protobuf.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * EWrapperImpl handles async callbacks from TWS.
 * Responsible for receiving the callback and dispatching to the correct action
 * The messages that we are interested in have a reqId parameter which is used to find
 * the action in the actions map and dispatch it. Any action may handle multiple callbacks
 * as part of the same logical request.
 */
public class EWrapperImpl implements EWrapper {
    private static final Logger log = LogManager.getLogger(EWrapperImpl.class.getSimpleName());

    //! [socket_declare]
    private final EReaderSignal readerSignal;
    private final EClientSocket clientSocket;
    protected int currentOrderId = -1;
    protected volatile int nextValidId = -1;
    protected final Map<Integer, Action> actions;
    //! [socket_declare]

    //! [socket_init]
    public EWrapperImpl(Map<Integer, Action> actions) {
        readerSignal = new EJavaSignal();
        clientSocket = new EClientSocket(this, readerSignal);
        this.actions = actions;
    }
    //! [socket_init]
    public EClientSocket getClient() {
        return clientSocket;
    }

    public EReaderSignal getSignal() {
        return readerSignal;
    }

    public int getCurrentOrderId() {
        return currentOrderId;
    }


    @Override
    public void tickPrice(int i, int i1, double v, TickAttrib tickAttrib) {

    }

    @Override
    public void tickSize(int i, int i1, Decimal decimal) {

    }

    @Override
    public void tickOptionComputation(int i, int i1, int i2, double v, double v1, double v2, double v3, double v4, double v5, double v6, double v7) {

    }

    @Override
    public void tickGeneric(int i, int i1, double v) {

    }

    @Override
    public void tickString(int i, int i1, String s) {

    }

    @Override
    public void tickEFP(int i, int i1, double v, String s, double v1, int i2, String s1, double v2, double v3) {

    }

    @Override
    public void orderStatus(int orderId, String status, Decimal filled, Decimal remaining, double avgFillPrice, long permId, int parentId,
                            double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {

    }

    @Override
    public void openOrder(int i, Contract contract, Order order, OrderState orderState) {

    }

    @Override
    public void openOrderEnd() {

    }

    @Override
    public void updateAccountValue(String s, String s1, String s2, String s3) {

    }

    @Override
    public void updatePortfolio(Contract contract, Decimal decimal, double v, double v1, double v2, double v3, double v4, String s) {

    }

    @Override
    public void updateAccountTime(String s) {

    }

    @Override
    public void accountDownloadEnd(String s) {

    }

    @Override
    public void nextValidId(int i) {
        nextValidId = i;
        log.info("nextValidId " + i);
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
        log.info("contractDetails reqId = {} {} {}", reqId, contractDetails.conid(), contractDetails.contract().textDescription());
        if (actions.get(reqId) instanceof ContractDetailsAction a) {
            log.info("found action in map");
            a.process(contractDetails);
        }
    }

    @Override
    public void bondContractDetails(int i, ContractDetails contractDetails) {

    }

    @Override
    public void contractDetailsEnd(int i) {
        log.info("contractDetailsEnd " + i);
        if (actions.get(i) instanceof ContractDetailsAction a) {
            log.info("found action in map");
            a.process();
        }
    }

    @Override
    public void execDetails(int i, Contract contract, Execution execution) {

    }

    @Override
    public void execDetailsEnd(int i) {

    }

    @Override
    public void updateMktDepth(int i, int i1, int i2, int i3, double v, Decimal decimal) {

    }

    @Override
    public void updateMktDepthL2(int i, int i1, String s, int i2, int i3, double v, Decimal decimal, boolean b) {

    }

    @Override
    public void updateNewsBulletin(int i, int i1, String s, String s1) {

    }

    @Override
    public void managedAccounts(String s) {

    }

    @Override
    public void receiveFA(int i, String s) {

    }

    // historical data is received bar by bar then an dataEnd message is sent
    @Override
    public void historicalData(int reqId, Bar bar) {
//        log.info("historicalData " + i);
        if (actions.get(reqId) instanceof HistoricalDataAction a) {
//            log.info("found action in map");
            a.process(bar);
        }
    }

    @Override
    public void historicalDataUpdate(int reqId, Bar bar) {
//        log.info("historicalDataUpdate " + i);
        if (actions.get(reqId) instanceof HistoricalDataAction a) {
//            log.info(bar.time() + " " + bar.close());
            a.processUpdate(bar);
        }
    }

    @Override
    public void historicalDataEnd(int reqId, String start, String end) {
        log.info("historicalDataEnd " + reqId + " " + start + " " + end);
        if (actions.get(reqId) instanceof HistoricalDataAction a) {
            a.processEnd();
        }
    }

    @Override
    public void scannerParameters(String s) {

    }

    @Override
    public void scannerData(int i, int i1, ContractDetails contractDetails, String s, String s1, String s2, String s3) {

    }

    @Override
    public void scannerDataEnd(int i) {

    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, Decimal volume, Decimal wap, int count) {
        if (actions.get(reqId) instanceof RealTimeBarsAction a) {
            var dt = LocalDateTime.ofInstant(new Date(time * 1000).toInstant(), ZoneId.of("UTC"));
            a.process(new RealTimeBar(dt, open, high, low, close, volume.longValue(), wap.longValue()));
        }
    }

    @Override
    public void currentTime(long l) {
        log.info("currentTime TWS server " + l + DateFormat.getDateTimeInstance().format(new Date(l * 1000)));
    }

    @Override
    public void fundamentalData(int i, String s) {

    }

    @Override
    public void deltaNeutralValidation(int i, DeltaNeutralContract deltaNeutralContract) {

    }

    @Override
    public void tickSnapshotEnd(int i) {

    }

    @Override
    public void marketDataType(int i, int i1) {

    }

    @Override
    public void commissionAndFeesReport(CommissionAndFeesReport commissionAndFeesReport) {
    }

    @Override
    public void position(String s, Contract contract, Decimal decimal, double v) {

    }

    @Override
    public void positionEnd() {

    }

    @Override
    public void accountSummary(int i, String s, String s1, String s2, String s3) {

    }

    @Override
    public void accountSummaryEnd(int i) {

    }

    @Override
    public void verifyMessageAPI(String s) {

    }

    @Override
    public void verifyCompleted(boolean b, String s) {

    }

    @Override
    public void verifyAndAuthMessageAPI(String s, String s1) {

    }

    @Override
    public void verifyAndAuthCompleted(boolean b, String s) {

    }

    @Override
    public void displayGroupList(int i, String s) {

    }

    @Override
    public void displayGroupUpdated(int i, String s) {

    }

    @Override
    public void error(Exception e) {
        log.error("ERROR: exception from API {}", e.getMessage());
    }

    @Override
    public void error(String e) {
        log.error("ERROR: error from API {}", e);
    }

    @Override
    public void error(int id, long errorTime, int errorCode, String errorMsg, String advancedOrderRejectJson) {
        // Common error codes:
        // 2104: Market data farm connection is OK
        // 2106: Historical data farm connection is OK
        // 2158: Sec-def data farm connection is OK
        // These are actually connectivity confirmations, not errors.
        if (errorCode == 2104 || errorCode == 2106 || errorCode == 2158) {
            log.info("Connectivity confirmation received: Code {}, Msg: {}", errorCode, errorMsg);
            return; // Not a true error
        }

        log.error("ERROR: Id: {}, time: {}, Code: {}, Msg: {}, Advanced Order Reject JSON: {}", id, errorCode, errorTime, errorMsg, advancedOrderRejectJson);

        //TODO: removed DSv3 code for now
        // If order placement error (e.g. id matches an orderId), update order book
//        if (id > 0) { // Often, id here is the orderId for order-related errors
//            if (errorCode == 201 || errorCode == 202) { // 201: Order rejected, 202: Order cancelled
//                orderBook.updateStatus(id, Status.REJECTED, 0); // Or CANCELLED as appropriate
//                log.info("Order {} rejected/cancelled via error callback. OrderBook: {}", id, orderBook.getSnapshot());
//            } else if (isConnectivityError(errorCode)) {
//                log.error("Connectivity issue detected (Code {}). Initiating disconnect.", errorCode);
//                handleDisconnect();
//            }
//        } else if (isConnectivityError(errorCode)) { // id == -1 for general messages
//            log.error("Connectivity issue detected (Code {}). Initiating disconnect.", errorCode);
//            handleDisconnect();
//        }
    }

    @Override
    public void connectionClosed() {

    }

    @Override
    public void connectAck() {

    }

    @Override
    public void positionMulti(int i, String s, String s1, Contract contract, Decimal decimal, double v) {

    }

    @Override
    public void positionMultiEnd(int i) {

    }

    @Override
    public void accountUpdateMulti(int i, String s, String s1, String s2, String s3, String s4) {

    }

    @Override
    public void accountUpdateMultiEnd(int i) {

    }

    @Override
    public void securityDefinitionOptionalParameter(int i, String s, int i1, String s1, String s2, Set<String> set, Set<Double> set1) {

    }

    @Override
    public void securityDefinitionOptionalParameterEnd(int i) {

    }

    @Override
    public void softDollarTiers(int i, SoftDollarTier[] softDollarTiers) {

    }

    @Override
    public void familyCodes(FamilyCode[] familyCodes) {

    }

    @Override
    public void symbolSamples(int i, ContractDescription[] contractDescriptions) {

    }

    @Override
    public void mktDepthExchanges(DepthMktDataDescription[] depthMktDataDescriptions) {

    }

    @Override
    public void tickNews(int i, long l, String s, String s1, String s2, String s3) {

    }

    @Override
    public void smartComponents(int i, Map<Integer, Map.Entry<String, Character>> map) {

    }

    @Override
    public void tickReqParams(int i, double v, String s, int i1) {

    }

    @Override
    public void newsProviders(NewsProvider[] newsProviders) {

    }

    @Override
    public void newsArticle(int i, int i1, String s) {

    }

    @Override
    public void historicalNews(int i, String s, String s1, String s2, String s3) {

    }

    @Override
    public void historicalNewsEnd(int i, boolean b) {

    }

    @Override
    public void headTimestamp(int i, String s) {

    }

    @Override
    public void histogramData(int i, List<HistogramEntry> list) {

    }

    @Override
    public void rerouteMktDataReq(int i, int i1, String s) {

    }

    @Override
    public void rerouteMktDepthReq(int i, int i1, String s) {

    }

    @Override
    public void marketRule(int i, PriceIncrement[] priceIncrements) {

    }

    @Override
    public void pnl(int i, double v, double v1, double v2) {

    }

    @Override
    public void pnlSingle(int i, Decimal decimal, double v, double v1, double v2, double v3) {

    }

    @Override
    public void historicalTicks(int i, List<HistoricalTick> list, boolean b) {

    }

    @Override
    public void historicalTicksBidAsk(int i, List<HistoricalTickBidAsk> list, boolean b) {

    }

    @Override
    public void historicalTicksLast(int i, List<HistoricalTickLast> list, boolean b) {

    }

    @Override
    public void tickByTickAllLast(int i, int i1, long l, double v, Decimal decimal, TickAttribLast tickAttribLast, String s, String s1) {

    }

    @Override
    public void tickByTickBidAsk(int i, long l, double v, double v1, Decimal decimal, Decimal decimal1, TickAttribBidAsk tickAttribBidAsk) {

    }

    @Override
    public void tickByTickMidPoint(int i, long l, double v) {

    }

    @Override
    public void orderBound(long l, int i, int i1) {

    }

    @Override
    public void completedOrder(Contract contract, Order order, OrderState orderState) {

    }

    @Override
    public void completedOrdersEnd() {

    }

    @Override
    public void replaceFAEnd(int i, String s) {

    }

    @Override
    public void wshMetaData(int i, String s) {

    }

    @Override
    public void wshEventData(int i, String s) {

    }

    @Override
    public void historicalSchedule(int i, String s, String s1, String s2, List<HistoricalSession> list) {

    }

    @Override
    public void userInfo(int i, String s) {

    }

    @Override
    public void currentTimeInMillis(long l) {

    }

    @Override
    public void orderStatusProtoBuf(OrderStatusProto.OrderStatus orderStatus) {

    }

    @Override
    public void openOrderProtoBuf(OpenOrderProto.OpenOrder openOrder) {

    }

    @Override
    public void openOrdersEndProtoBuf(OpenOrdersEndProto.OpenOrdersEnd openOrdersEnd) {

    }

    @Override
    public void errorProtoBuf(ErrorMessageProto.ErrorMessage errorMessage) {

    }

    @Override
    public void execDetailsProtoBuf(ExecutionDetailsProto.ExecutionDetails executionDetails) {

    }

    @Override
    public void execDetailsEndProtoBuf(ExecutionDetailsEndProto.ExecutionDetailsEnd executionDetailsEnd) {

    }
}
