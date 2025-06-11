package ibhist;

import com.ib.client.EClientSocket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract  class ActionBase implements Action {
    protected static final Logger log = LogManager.getLogger(ActionBase.class.getSimpleName());
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

    /**
     * called once the action has been completed.
     * puts this onto queue back to main thread.
     * the concrete class will have the data received.
     */
    @Override
    public void complete() {
        try {
            queue.put(this);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void cancel() {}

    @Override
    public String toString() {
        return getClass().getSimpleName() + " " + requestId;
    }
}
