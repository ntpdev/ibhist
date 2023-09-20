package ibhist;

import com.ib.client.EClientSocket;

import java.util.concurrent.atomic.AtomicInteger;

public abstract  class ActionBase implements Action {
    protected final int requestId;
    protected final EClientSocket client;

    public ActionBase(EClientSocket client, AtomicInteger idGenerator) {
        requestId = idGenerator.getAndIncrement();
        this.client = client;
    }

    public int getRequestId() {
        return requestId;
    }

    @Override
    public void cancel() {}
}
