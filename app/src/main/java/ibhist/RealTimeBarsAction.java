package ibhist;

import com.ib.client.Contract;
import com.ib.client.EClientSocket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoublePredicate;

public class RealTimeBarsAction extends ActionBase {
    private final Contract contract;
    private final RealTimeHistory bars = new RealTimeHistory();
    private final List<DoublePredicate> monitors = new ArrayList<>();
    private final MonitorManager monitorManager;
    private final int maxBars;
    private volatile boolean cancelSent = false;
    private boolean fInit = false;

    public RealTimeBarsAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract, MonitorManager monitorManager, int maxBars) {
        super(client, idGenerator, queue);
        this.contract = contract;
        this.monitorManager = monitorManager;
        this.maxBars = maxBars;
    }

    private void priceTriggered(RealTimeHistory.PriceEvent event) {
        log.info(event);
    }

    @Override
    public void makeRequest() {
        log.info("reqRealTimeBars reqId = {} symbol = {}", requestId, contract.localSymbol());
        client.reqRealTimeBars(requestId, contract, 5, "TRADES", false, Collections.emptyList());
    }


    @Override
    public void cancel() {
        client.cancelRealTimeBars(requestId);
    }

    public void forceCancel() {
        cancelSent = true;
    }

    public void onRealtimeBar(RealTimeBar bar) {
//        log.info("process received " + bar);
        if (!fInit) {
            fInit = true;
            for (var monitor : monitorManager) {
                monitors.add(RealTimeHistory.newPriceMonitor(monitor, this::priceTriggered));
            }
        }
        bars.add(bar);
        monitorPrice(bar);
        if (bar.dt().getSecond() == 0) {
            var xs = bars.toPriceBars();

            StringBuilder sb = new StringBuilder();
            sb.append("\n[yellow]--- real time history ").append(xs.getLast().start().toLocalTime()).append("[/]\n");
            double prevHi = xs.getFirst().high();
            double prevLo = xs.getFirst().low();
            for (var b : xs) {
                sb.append(b.asIntradayBar(b.high() > prevHi, b.low() < prevLo, false))
                        .append(System.lineSeparator());
                prevLo = b.low();
                prevHi = b.high();
            }
            StringUtils.print(sb);
        }
//        log.info("running m1 " + bars.aggregrateLast(12));

        // cancel realtime bars after limit or cancel sent.
        if (bars.size() >= maxBars || cancelSent) {
            cancel();
            complete();  // put action on queue
        }
    }

    protected void monitorPrice(RealTimeBar bar) {
        for (var n : monitors) {
            n.test(bar.close());
        }
    }

}
