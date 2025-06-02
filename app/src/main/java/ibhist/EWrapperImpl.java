package ibhist;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ib.client.*;
import com.ib.client.protobuf.ErrorMessageProto;
import com.ib.client.protobuf.ExecutionDetailsEndProto;
import com.ib.client.protobuf.ExecutionDetailsProto;
import com.ib.client.protobuf.OpenOrderProto;
import com.ib.client.protobuf.OpenOrdersEndProto;
import com.ib.client.protobuf.OrderStatusProto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;

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
	private EReaderSignal readerSignal;
	private EClientSocket clientSocket;
	protected int currentOrderId = -1;
	//! [socket_declare]
	protected final Map<Integer, Action> actions;


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

	public <T extends Action> T findByType(Class<T> clz) {
		for (Action action : actions.values()) {
			if (clz.isInstance(action)) {
				return clz.cast(action);
			}
		}
		return null;
	}

	static void logNotFound(int reqId) {
		log.error("ERROR: reqId = {} not found in actions map ", reqId);
	}

	 //! [tickprice]
	@Override
	public void tickPrice(int tickerId, int field, double price, TickAttrib attribs) {
		log.info("Tick Price: " + EWrapperMsgGenerator.tickPrice( tickerId, field, price, attribs));
	}
	//! [tickprice]
	
	//! [ticksize]
	@Override
	public void tickSize(int tickerId, int field, Decimal size) {
		log.info("Tick Size: " + EWrapperMsgGenerator.tickSize( tickerId, field, size));
	}
	//! [ticksize]
	
	//! [tickoptioncomputation]
	@Override
	public void tickOptionComputation(int tickerId, int field, int tickAttrib, double impliedVol, double delta, double optPrice,
			double pvDividend, double gamma, double vega, double theta, double undPrice) {
		log.info("TickOptionComputation: " + EWrapperMsgGenerator.tickOptionComputation( tickerId, field, tickAttrib, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice));
	}
	//! [tickoptioncomputation]
	
	//! [tickgeneric]
	@Override
	public void tickGeneric(int tickerId, int tickType, double value) {
		log.info("Tick Generic: " + EWrapperMsgGenerator.tickGeneric(tickerId, tickType, value));
	}
	//! [tickgeneric]
	
	//! [tickstring]
	@Override
	public void tickString(int tickerId, int tickType, String value) {
		log.info("Tick String: " + EWrapperMsgGenerator.tickString(tickerId, tickType, value));
	}
	//! [tickstring]
	@Override
	public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays,
			String futureLastTradeDate, double dividendImpact, double dividendsToLastTradeDate) {
		log.info("TickEFP: " + EWrapperMsgGenerator.tickEFP(tickerId, tickType, basisPoints, formattedBasisPoints,
				impliedFuture, holdDays, futureLastTradeDate, dividendImpact, dividendsToLastTradeDate));
	}
	//! [orderstatus]
	@Override
	public void orderStatus(int orderId, String status, Decimal filled, Decimal remaining, double avgFillPrice, long permId, int parentId,
			double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
		log.info(EWrapperMsgGenerator.orderStatus( orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice));
	}
	//! [orderstatus]
	
	//! [openorder]
	@Override
	public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {
		log.info(EWrapperMsgGenerator.openOrder(orderId, contract, order, orderState));
	}
	//! [openorder]
	
	//! [openorderend]
	@Override
	public void openOrderEnd() {
		log.info("Open Order End: " + EWrapperMsgGenerator.openOrderEnd());
	}
	//! [openorderend]
	
	//! [updateaccountvalue]
	@Override
	public void updateAccountValue(String key, String value, String currency, String accountName) {
		log.info(EWrapperMsgGenerator.updateAccountValue( key, value, currency, accountName));
	}
	//! [updateaccountvalue]
	
	//! [updateportfolio]
	@Override
	public void updatePortfolio(Contract contract, Decimal position, double marketPrice, double marketValue, double averageCost,
			double unrealizedPNL, double realizedPNL, String accountName) {
		log.info(EWrapperMsgGenerator.updatePortfolio( contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName));
	}
	//! [updateportfolio]
	
	//! [updateaccounttime]
	@Override
	public void updateAccountTime(String timeStamp) {
		log.info(EWrapperMsgGenerator.updateAccountTime( timeStamp));
	}
	//! [updateaccounttime]
	
	//! [accountdownloadend]
	@Override
	public void accountDownloadEnd(String accountName) {
		log.info(EWrapperMsgGenerator.accountDownloadEnd(accountName));
	}
	//! [accountdownloadend]
	
	//! [nextvalidid]
	@Override
	public void nextValidId(int orderId) {
		log.info(EWrapperMsgGenerator.nextValidId(orderId));
		currentOrderId = orderId;
		// No req id for this message - unsolicited
		NextOrderIdAction a;
		if ((a = findByType(NextOrderIdAction.class)) != null) {
			a.process(orderId);
		}
	}
	//! [nextvalidid]
	
	//! [contractdetails]
	@Override
	public void contractDetails(int reqId, ContractDetails contractDetails) {
		log.info("contractDetails reqId = {} {} {}", reqId, contractDetails.conid(), contractDetails.contract().textDescription());
		if (actions.get(reqId) instanceof ContractDetailsAction action) {
			action.process(contractDetails);
		} else {
			logNotFound(reqId);
		}
	}

	//! [contractdetails]
	@Override
	public void bondContractDetails(int reqId, ContractDetails contractDetails) {
		log.info(EWrapperMsgGenerator.bondContractDetails(reqId, contractDetails)); 
	}
	//! [contractdetailsend]
	@Override
	public void contractDetailsEnd(int reqId) {
		log.info("contractDetailsEnd reqId = " + reqId);
		if (actions.get(reqId) instanceof ContractDetailsAction action) {
			action.complete();
		} else {
			logNotFound(reqId);
		}
	}
	//! [contractdetailsend]
	
	//! [execdetails]
	@Override
	public void execDetails(int reqId, Contract contract, Execution execution) {
		log.info(EWrapperMsgGenerator.execDetails( reqId, contract, execution));
	}
	//! [execdetails]
	
	//! [execdetailsend]
	@Override
	public void execDetailsEnd(int reqId) {
		log.info("Exec Details End: " + EWrapperMsgGenerator.execDetailsEnd( reqId));
	}
	//! [execdetailsend]
	
	//! [updatemktdepth]
	@Override
	public void updateMktDepth(int tickerId, int position, int operation, int side, double price, Decimal size) {
		log.info(EWrapperMsgGenerator.updateMktDepth(tickerId, position, operation, side, price, size));
	}
	//! [updatemktdepth]
	
	//! [updatemktdepthl2]
	@Override
	public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation, int side, double price, Decimal size, boolean isSmartDepth) {
		log.info(EWrapperMsgGenerator.updateMktDepthL2( tickerId, position, marketMaker, operation, side, price, size, isSmartDepth));
	}
	//! [updatemktdepthl2]
	
	//! [updatenewsbulletin]
	@Override
	public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {
		log.info("News Bulletin: " + EWrapperMsgGenerator.updateNewsBulletin( msgId, msgType, message, origExchange));
	}
	//! [updatenewsbulletin]
	
	//! [managedaccounts]
	@Override
	public void managedAccounts(String accountsList) {
		log.info("Account list: " + accountsList);
	}
	//! [managedaccounts]

	//! [receivefa]
	@Override
	public void receiveFA(int faDataType, String xml) {
		log.info("Receiving FA: " + faDataType + " - " + xml);
	}
	//! [receivefa]
	
	//! [historicaldata]
	@Override
	public void historicalData(int reqId, Bar bar) {
		if (actions.get(reqId) instanceof HistoricalDataAction action) {
			action.process(bar);
		} else {
			logNotFound(reqId);
		}
	}
	//! [historicaldata]
	
	//! [historicaldataend]
	@Override
	public void historicalDataEnd(int reqId, String startDateStr, String endDateStr) {
		log.info("historicalDataEnd reqId = {} {} {}", reqId, startDateStr, endDateStr);
		if (actions.get(reqId) instanceof HistoricalDataAction action) {
			action.processEnd();
		} else {
			logNotFound(reqId);
		}
	}
	//! [historicaldataend]
	
	
	//! [scannerparameters]
	@Override
	public void scannerParameters(String xml) {
		log.info("ScannerParameters. " + xml + "\n");
	}
	//! [scannerparameters]
	
	//! [scannerdata]
	@Override
	public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {
		log.info("ScannerData: " + EWrapperMsgGenerator.scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr));
	}
	//! [scannerdata]
	
	//! [scannerdataend]
	@Override
	public void scannerDataEnd(int reqId) {
		log.info("ScannerDataEnd: " + EWrapperMsgGenerator.scannerDataEnd(reqId));
	}
	//! [scannerdataend]
	
	//! [realtimebar]
	@Override
	public void realtimeBar(int reqId, long time, double open, double high, double low, double close, Decimal volume, Decimal wap, int count) {
		if (actions.get(reqId) instanceof RealTimeBarsAction a) {
			var dt = LocalDateTime.ofInstant(new Date(time * 1000).toInstant(), ZoneId.of("UTC"));
			a.process(new RealTimeBar(dt, open, high, low, close, volume.longValue(), wap.longValue()));
		} else {
			logNotFound(reqId);
		}
	}
	//! [realtimebar]
	@Override
	public void currentTime(long time) {
		log.info("currentTime TWS server " + time + DateFormat.getDateTimeInstance().format(new Date(time * 1000)));

	}
	//! [fundamentaldata]
	@Override
	public void fundamentalData(int reqId, String data) {
		log.info("FundamentalData: " + EWrapperMsgGenerator.fundamentalData(reqId, data));
	}
	//! [fundamentaldata]
	@Override
	public void deltaNeutralValidation(int reqId, DeltaNeutralContract deltaNeutralContract) {
		log.info("Delta Neutral Validation: " + EWrapperMsgGenerator.deltaNeutralValidation(reqId, deltaNeutralContract));
	}
	//! [ticksnapshotend]
	@Override
	public void tickSnapshotEnd(int reqId) {
		log.info("TickSnapshotEnd: " + EWrapperMsgGenerator.tickSnapshotEnd(reqId));
	}
	//! [ticksnapshotend]
	
	//! [marketdatatype]
	@Override
	public void marketDataType(int reqId, int marketDataType) {
		log.info("MarketDataType: " + EWrapperMsgGenerator.marketDataType(reqId, marketDataType));
	}
	//! [marketdatatype]
	
	//! [commissionandfeesreport]
	@Override
	public void commissionAndFeesReport(CommissionAndFeesReport commissionAndFeesReport) {
		log.info(EWrapperMsgGenerator.commissionAndFeesReport(commissionAndFeesReport));
	}
	//! [commissionandfeesreport]
	
	//! [position]
	@Override
	public void position(String account, Contract contract, Decimal pos, double avgCost) {
		log.info(EWrapperMsgGenerator.position(account, contract, pos, avgCost));
	}
	//! [position]
	
	//! [positionend]
	@Override
	public void positionEnd() {
		log.info("Position End: " + EWrapperMsgGenerator.positionEnd());
	}
	//! [positionend]
	
	//! [accountsummary]
	@Override
	public void accountSummary(int reqId, String account, String tag, String value, String currency) {
		log.info(EWrapperMsgGenerator.accountSummary(reqId, account, tag, value, currency));
	}
	//! [accountsummary]
	
	//! [accountsummaryend]
	@Override
	public void accountSummaryEnd(int reqId) {
		log.info("Account Summary End. Req Id: " + EWrapperMsgGenerator.accountSummaryEnd(reqId));
	}
	//! [accountsummaryend]
	@Override
	public void verifyMessageAPI(String apiData) {
		log.info("verifyMessageAPI");
	}

	@Override
	public void verifyCompleted(boolean isSuccessful, String errorText) {
		log.info("verifyCompleted");
	}

	@Override
	public void verifyAndAuthMessageAPI(String apiData, String xyzChallenge) {
		log.info("verifyAndAuthMessageAPI");
	}

	@Override
	public void verifyAndAuthCompleted(boolean isSuccessful, String errorText) {
		log.info("verifyAndAuthCompleted");
	}
	//! [displaygrouplist]
	@Override
	public void displayGroupList(int reqId, String groups) {
		log.info("Display Group List. ReqId: " + reqId + ", Groups: " + groups + "\n");
	}
	//! [displaygrouplist]
	
	//! [displaygroupupdated]
	@Override
	public void displayGroupUpdated(int reqId, String contractInfo) {
		log.info("Display Group Updated. ReqId: " + reqId + ", Contract info: " + contractInfo + "\n");
	}
	//! [displaygroupupdated]
	@Override
	public void error(Exception e) {
		log.info("Exception: " + e.getMessage());
	}

	@Override
	public void error(String str) {
		log.info("Error: " + str);
	}
	//! [error]
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

		String str = "Error. Id: %s, Time: %s, Code: %s, Msg: %s AdvOrderRejectJson: %s".formatted(
				id,
				errorTime > 0 ? Util.UnixMillisecondsToString(errorTime, "yyyyMMdd-HH:mm:ss") : "-",
				errorCode,
				errorMsg,
				advancedOrderRejectJson);
		log.info(str);
	}
	//! [error]
	@Override
	public void connectionClosed() {
		log.info("Connection closed");
	}

	//! [connectack]
	@Override
	public void connectAck() {
		if (clientSocket.isAsyncEConnect()) {
			log.info("Acknowledging connection");
			clientSocket.startAPI();
		}
	}
	//! [connectack]
	
	//! [positionmulti]
	@Override
	public void positionMulti(int reqId, String account, String modelCode, Contract contract, Decimal pos, double avgCost) {
		log.info(EWrapperMsgGenerator.positionMulti(reqId, account, modelCode, contract, pos, avgCost));
	}
	//! [positionmulti]
	
	//! [positionmultiend]
	@Override
	public void positionMultiEnd(int reqId) {
		log.info("Position Multi End: " + EWrapperMsgGenerator.positionMultiEnd(reqId));
	}
	//! [positionmultiend]
	
	//! [accountupdatemulti]
	@Override
	public void accountUpdateMulti(int reqId, String account, String modelCode, String key, String value, String currency) {
		log.info("Account Update Multi: " + EWrapperMsgGenerator.accountUpdateMulti(reqId, account, modelCode, key, value, currency));
	}
	//! [accountupdatemulti]
	
	//! [accountupdatemultiend]
	@Override
	public void accountUpdateMultiEnd(int reqId) {
		log.info("Account Update Multi End: " + EWrapperMsgGenerator.accountUpdateMultiEnd(reqId));
	}
	//! [accountupdatemultiend]
	
	//! [securityDefinitionOptionParameter]
	@Override
	public void securityDefinitionOptionalParameter(int reqId, String exchange, int underlyingConId, String tradingClass, String multiplier,
			Set<String> expirations, Set<Double> strikes) {
		log.info("Security Definition Optional Parameter: " + EWrapperMsgGenerator.securityDefinitionOptionalParameter(reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes));
	}
	//! [securityDefinitionOptionParameter]

	//! [securityDefinitionOptionParameterEnd]
	@Override
	public void securityDefinitionOptionalParameterEnd(int reqId) {
		log.info("Security Definition Optional Parameter End. Request Id: " + reqId);
	}
	//! [securityDefinitionOptionParameterEnd]

    //! [softDollarTiers]
	@Override
	public void softDollarTiers(int reqId, SoftDollarTier[] tiers) {
		log.info(EWrapperMsgGenerator.softDollarTiers(tiers));
	}
    //! [softDollarTiers]

    //! [familyCodes]
    @Override
    public void familyCodes(FamilyCode[] familyCodes) {
        log.info(EWrapperMsgGenerator.familyCodes(familyCodes));
    }
    //! [familyCodes]
    
    //! [symbolSamples]
    @Override
    public void symbolSamples(int reqId, ContractDescription[] contractDescriptions) {
        log.info(EWrapperMsgGenerator.symbolSamples(reqId, contractDescriptions));
    }
    //! [symbolSamples]
    
	//! [mktDepthExchanges]
	@Override
	public void mktDepthExchanges(DepthMktDataDescription[] depthMktDataDescriptions) {
		log.info(EWrapperMsgGenerator.mktDepthExchanges(depthMktDataDescriptions));
	}
	//! [mktDepthExchanges]
	
	//! [tickNews]
	@Override
	public void tickNews(int tickerId, long timeStamp, String providerCode, String articleId, String headline, String extraData) {
		log.info(EWrapperMsgGenerator.tickNews(tickerId, timeStamp, providerCode, articleId, headline, extraData));
	}
	//! [tickNews]

	//! [smartcomponents]
	@Override
	public void smartComponents(int reqId, Map<Integer, Entry<String, Character>> theMap) {
		log.info(EWrapperMsgGenerator.smartComponents(reqId, theMap));
	}
	//! [smartcomponents]

	//! [tickReqParams]
	@Override
	public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {
		log.info("Tick req params: " + EWrapperMsgGenerator.tickReqParams(tickerId, minTick, bboExchange, snapshotPermissions));
	}
	//! [tickReqParams]

	//! [newsProviders]
	@Override
	public void newsProviders(NewsProvider[] newsProviders) {
		log.info(EWrapperMsgGenerator.newsProviders(newsProviders));
	}
	//! [newsProviders]

	//! [newsArticle]
	@Override
	public void newsArticle(int requestId, int articleType, String articleText) {
		log.info(EWrapperMsgGenerator.newsArticle(requestId, articleType, articleText));
	}
	//! [newsArticle]

	//! [historicalNews]
	@Override
	public void historicalNews(int requestId, String time, String providerCode, String articleId, String headline) {
		log.info(EWrapperMsgGenerator.historicalNews(requestId, time, providerCode, articleId, headline));
	}
	//! [historicalNews]

	//! [historicalNewsEnd]
	@Override
	public void historicalNewsEnd(int requestId, boolean hasMore) {
		log.info(EWrapperMsgGenerator.historicalNewsEnd(requestId, hasMore));
	}
	//! [historicalNewsEnd]

	//! [headTimestamp]
	@Override
	public void headTimestamp(int reqId, String headTimestamp) {
		log.info(EWrapperMsgGenerator.headTimestamp(reqId, headTimestamp));
	}
	//! [headTimestamp]
	
	//! [histogramData]
	@Override
	public void histogramData(int reqId, List<HistogramEntry> items) {
		log.info(EWrapperMsgGenerator.histogramData(reqId, items));
	}
	//! [histogramData]

	//! [historicalDataUpdate]
	@Override
    public void historicalDataUpdate(int reqId, Bar bar) {
//		log.info("historicalDataUpdate reqId = " + reqId);
		if (actions.get(reqId) instanceof HistoricalDataAction action) {
			action.processUpdate(bar);
		} else {
			logNotFound(reqId);
		}
    }
	//! [historicalDataUpdate]
	
	//! [rerouteMktDataReq]
	@Override
	public void rerouteMktDataReq(int reqId, int conId, String exchange) {
		log.info(EWrapperMsgGenerator.rerouteMktDataReq(reqId, conId, exchange));
	}
	//! [rerouteMktDataReq]
	
	//! [rerouteMktDepthReq]
	@Override
	public void rerouteMktDepthReq(int reqId, int conId, String exchange) {
		log.info(EWrapperMsgGenerator.rerouteMktDepthReq(reqId, conId, exchange));
	}
	//! [rerouteMktDepthReq]
	
	//! [marketRule]
	@Override
	public void marketRule(int marketRuleId, PriceIncrement[] priceIncrements) {
		log.info(EWrapperMsgGenerator.marketRule(marketRuleId, priceIncrements));
	}
	//! [marketRule]
	
	//! [pnl]
    @Override
    public void pnl(int reqId, double dailyPnL, double unrealizedPnL, double realizedPnL) {
        log.info(EWrapperMsgGenerator.pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL));
    }
    //! [pnl]
	
	//! [pnlsingle]
    @Override
    public void pnlSingle(int reqId, Decimal pos, double dailyPnL, double unrealizedPnL, double realizedPnL, double value) {
        log.info(EWrapperMsgGenerator.pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value));                
    }
    //! [pnlsingle]
	
	//! [historicalticks]
    @Override
    public void historicalTicks(int reqId, List<HistoricalTick> ticks, boolean done) {
        for (HistoricalTick tick : ticks) {
            log.info(EWrapperMsgGenerator.historicalTick(reqId, tick.time(), tick.price(), tick.size()));
        }
    }
    //! [historicalticks]
	
	//! [historicalticksbidask]
    @Override
    public void historicalTicksBidAsk(int reqId, List<HistoricalTickBidAsk> ticks, boolean done) {
        for (HistoricalTickBidAsk tick : ticks) {
            log.info(EWrapperMsgGenerator.historicalTickBidAsk(reqId, tick.time(), tick.tickAttribBidAsk(), tick.priceBid(), tick.priceAsk(), tick.sizeBid(),
                    tick.sizeAsk()));
        }
    }   
    //! [historicalticksbidask]
	
    @Override
	//! [historicaltickslast]
    public void historicalTicksLast(int reqId, List<HistoricalTickLast> ticks, boolean done) {
        for (HistoricalTickLast tick : ticks) {
            log.info(EWrapperMsgGenerator.historicalTickLast(reqId, tick.time(), tick.tickAttribLast(), tick.price(), tick.size(), tick.exchange(), 
                tick.specialConditions()));
        }
    }
    //! [historicaltickslast]

    //! [tickbytickalllast]
   @Override
    public void tickByTickAllLast(int reqId, int tickType, long time, double price, Decimal size, TickAttribLast tickAttribLast,
            String exchange, String specialConditions) {
        log.info(EWrapperMsgGenerator.tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions));
    }
    //! [tickbytickalllast]

    //! [tickbytickbidask]
    @Override
    public void tickByTickBidAsk(int reqId, long time, double bidPrice, double askPrice, Decimal bidSize, Decimal askSize,
            TickAttribBidAsk tickAttribBidAsk) {
        log.info(EWrapperMsgGenerator.tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk));
    }
    //! [tickbytickbidask]
    
    //! [tickbytickmidpoint]
    @Override
    public void tickByTickMidPoint(int reqId, long time, double midPoint) {
        log.info(EWrapperMsgGenerator.tickByTickMidPoint(reqId, time, midPoint));
    }
    //! [tickbytickmidpoint]

    //! [orderbound]
    @Override
    public void orderBound(long permId, int clientId, int orderId) {
        log.info(EWrapperMsgGenerator.orderBound(permId, clientId, orderId));
    }
    //! [orderbound]

    //! [completedorder]
    @Override
    public void completedOrder(Contract contract, Order order, OrderState orderState) {
        log.info(EWrapperMsgGenerator.completedOrder(contract, order, orderState));
    }
    //! [completedorder]

    //! [completedordersend]
    @Override
    public void completedOrdersEnd() {
        log.info(EWrapperMsgGenerator.completedOrdersEnd());
    }
    //! [completedordersend]

    //! [replacefaend]
    @Override
    public void replaceFAEnd(int reqId, String text) {
        log.info(EWrapperMsgGenerator.replaceFAEnd(reqId, text));
    }
    //! [replacefaend]
    
    //! [wshMetaData]
	@Override
	public void wshMetaData(int reqId, String dataJson) {
		log.info(EWrapperMsgGenerator.wshMetaData(reqId, dataJson));
	}
    //! [wshMetaData]

    //! [wshEventData]
	@Override
	public void wshEventData(int reqId, String dataJson) {
        log.info(EWrapperMsgGenerator.wshEventData(reqId, dataJson));
	}
    //! [wshEventData]

    //! [historicalSchedule]
    @Override
    public void historicalSchedule(int reqId, String startDateTime, String endDateTime, String timeZone, List<HistoricalSession> sessions) {
        log.info(EWrapperMsgGenerator.historicalSchedule(reqId, startDateTime, endDateTime, timeZone, sessions));
    }
    //! [historicalSchedule]
    
    //! [userInfo]
    @Override
    public void userInfo(int reqId, String whiteBrandingId) {
        log.info(EWrapperMsgGenerator.userInfo(reqId, whiteBrandingId));
    }
    //! [userInfo]

    //! [currentTimeInMillis]
    @Override
    public void currentTimeInMillis(long timeInMillis) {
        log.info(EWrapperMsgGenerator.currentTimeInMillis(timeInMillis));
    }
    //! [currentTimeInMillis]
    
    // ---------------------------------------------- Protobuf ---------------------------------------------
    //! [orderStatus]
    @Override public void orderStatusProtoBuf(OrderStatusProto.OrderStatus orderStatusProto) { }
    //! [orderStatus]

    //! [openOrder]
    @Override public void openOrderProtoBuf(OpenOrderProto.OpenOrder openOrderProto) { }
    //! [openOrder]

    //! [openOrdersEnd]
    @Override public void openOrdersEndProtoBuf(OpenOrdersEndProto.OpenOrdersEnd openOrdersEnd) { }
    //! [openOrdersEnd]

    //! [error]
    @Override public void errorProtoBuf(ErrorMessageProto.ErrorMessage errorMessageProto) { }
    //! [error]

    //! [execDetails]
    @Override public void execDetailsProtoBuf(ExecutionDetailsProto.ExecutionDetails executionDetailsProto) { }
    //! [execDetails]

    //! [execDetailsEnd]
    @Override public void execDetailsEndProtoBuf(ExecutionDetailsEndProto.ExecutionDetailsEnd executionDetailsEndProto) { }
    //! [execDetailsEnd]
}
