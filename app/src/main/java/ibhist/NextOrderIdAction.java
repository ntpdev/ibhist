package ibhist;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

public class NextOrderIdAction extends ActionBase {
    private int orderId;

    @AssistedInject
    public NextOrderIdAction(@Assisted ActionContext context) {
        super(context.client(), context.idGenerator(), context.queue());
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
