package ibhist;

import com.ib.client.EClientSocket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract  class ActionBase implements Action {
    protected static final Logger log = LogManager.getLogger("ActionBase");
    protected final int requestId;
    protected final EClientSocket client;
    protected final BlockingQueue<Action> queue;

    public ActionBase(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue) {
        requestId = idGenerator.getAndIncrement();
        this.client = client;
        this.queue = queue;
    }

    public int getRequestId() {
        return requestId;
    }

    @Override
    public void process() {
        try {
            queue.put(this);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void cancel() {}
}
