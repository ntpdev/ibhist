package ibhist;

import com.google.common.math.DoubleMath;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RealTimeBarsAction extends ActionBase {
    private final Contract contract;
    private final RealTimeHistory bars = new RealTimeHistory();
    private final RealTimeHistory.PriceCondition countNotifier;

    public RealTimeBarsAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract) {
        super(client, idGenerator, queue);
        this.contract = contract;
        countNotifier = RealTimeHistory.newCountNotifier(e -> e > 5906.00, 3, this::priceTriggered);
    }

    private void priceTriggered(RealTimeHistory.PriceEvent event) {
        log.info(event);
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
        countNotifier.test(bar.close());
        if (bar.dt().getSecond() == 0) {
            var xs = bars.toPriceBars();

            StringBuilder sb = new StringBuilder();
            sb.append(System.lineSeparator()).append(PriceHistory.ANSI_YELLOW + "--- real time history" + PriceHistory.ANSI_RESET).append(System.lineSeparator());
            double prevHi = xs.getFirst().high();
            double prevLo = xs.getFirst().low();
            for (var b: xs) {
                sb.append(b.asIntradayBar(b.high() > prevHi, b.low() < prevLo, false))
                        .append(System.lineSeparator());
                prevLo = b.low();
                prevHi = b.high();
            }
            System.out.println(sb);
        }
//        log.info("running m1 " + bars.aggregrateLast(12));

        if (bars.size() > 120) {
            cancel();
            process();  // put action on queue
        }
    }

}
