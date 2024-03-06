package ibhist;

import com.ib.client.Contract;
import com.ib.client.EClientSocket;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RealTimeBarsAction extends ActionBase {
    private final Contract contract;
    private final RealTimeHistory bars = new RealTimeHistory();

    public RealTimeBarsAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract) {
        super(client, idGenerator, queue);
        this.contract = contract;
    }

    @Override
    public void makeRequest() {
        client.reqRealTimeBars(requestId, contract, 5, "TRADES", false, Collections.emptyList());
    }

    @Override
    public void cancel() {
        client.cancelRealTimeBars(requestId);
    }

    public void process(RealTimeBar bar) {
//        log.info("process received " + bar);
        bars.add(bar);
        if (bar.dt().getSecond() == 0) {
            log.info("aggregrate m2 " + bars.aggregrateLast(24));
        }
//        log.info("running m1 " + bars.aggregrateLast(12));

        if (bars.size() > 120) {
            cancel();
            process();  // put action on queue
        }
    }

}
