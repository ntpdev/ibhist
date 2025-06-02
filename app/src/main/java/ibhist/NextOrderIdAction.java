package ibhist;

import com.ib.client.EClientSocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class NextOrderIdAction extends ActionBase {
    private int orderId;
    public NextOrderIdAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue) {
        super(client, idGenerator, queue);
    }

    public int getOrderId() {
        return orderId;
    }

    void process(int orderId) {
        this.orderId = orderId;
        complete();
    }

    @Override
    public void makeRequest() { // NOOP }
    }
}
