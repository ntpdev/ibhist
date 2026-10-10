package ibhist;

import com.ib.client.Contract;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Assisted injection factory for the per request TWS actions.
 * Container owned dependencies (EventService, MonitorManager) are injected, everything
 * else is supplied by the caller for each request.
 */
public interface ActionFactory {

    NextOrderIdAction nextOrderId(ActionContext context);

    OrderManagerAction orderManager(ActionContext context);

    ContractDetailsAction contractDetails(ActionContext context, Contract contract);

    /**
     * @param endDate last bar to fetch or empty for the most recent data
     */
    HistoricalDataAction historicalData(ActionContext context, Contract contract, Optional<LocalDate> endDate,
                                        Duration duration, boolean keepUpToDate);

    RealTimeBarsAction realTimeBars(ActionContext context, Contract contract, int maxBars);
}