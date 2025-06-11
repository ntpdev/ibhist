package ibhist;

import com.ib.client.Contract;
import com.ib.client.EClientSocket;
import com.ib.client.Order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderManagerAction extends ActionBase {
    Map<Long, OrderInfo> orderMap = new HashMap<>();

    public OrderManagerAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue) {
        super(client, idGenerator, queue);
    }

    public OrderInfo onOpenOrder(Order order, Contract contract) {
        long id = order.orderId() > 0 ? order.orderId() : order.permId();
        OrderInfo info = orderMap.get(id);
        if (info == null) {
            info = new OrderInfo(order, contract, new ArrayList<>());
            log.info("order {} added {} {}", info.id(), info.order().action(), info.contract.symbol());
            orderMap.put(info.id(), info);
        } else {
            log.info("order {} updated {} {}", info.id(), info.order().action(), info.contract.symbol());
            info.updateOrder(order);
        }
        return info;
    }

    public OrderInfo onOrderStatus(OrderStatus status) {
        long id = status.id();
        OrderInfo info = orderMap.get(id);
        if (info == null) {
            log.info("order {} status for order not found", status.id());
            return info;
        }
        log.info("order {} status updated", status.id());
        info.statusHistory().add(status);
        return info;
    }


    @Override
    public void makeRequest() {
        // noop
    }

    @Override
    public String toString() {
        return "OrderManagerAction{" +
                "orderMap=" + orderMap.size() +
                '}';
    }

    // records this history of a given order
    public record OrderInfo(
            long orderId,
            List<Changed<Order>> orderHistory,
            Contract contract,
            List<OrderStatus> statusHistory
    ) {
        public OrderInfo(
                Order order,
                Contract contract,
                List<OrderStatus> statusHistory
        ) {
            this(order.orderId() > 0 ? order.orderId() : order.permId(),
                    new ArrayList<>(),
                    contract,
                    statusHistory);
            orderHistory.add(new Changed<>(order));
        }

        // returns current order state
        Order order() {
            return orderHistory.getLast().getValue();
        }

        void updateOrder(Order order) {
            orderHistory.add(new Changed<>(order));
        }

        public long id() {
            return orderId;
        }

        public String symbol() {
            String s1 = contract.symbol();
            String s2 = contract.localSymbol();
            return s2.length() > s1.length() ? s2 : s1;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(id());
        }

        @Override
        public String toString() {
            var order = order();
            return "OrderInfo{" +
                    "id=" + id() +
                    ", order=" + order.getAction() +
                    ", order=" + order.lmtPrice() +
                    ", order=" + order.tif() +
                    ", changes=" + orderHistory.size() +
                    '}';
        }
    }


    // nothing interesting in this message - stored but not used
    public record OrderStatus(
            int orderId,
            String status,
            BigDecimal filled,
            BigDecimal remaining,
            double avgFillPrice,
            long permId,
            int parentId,
            double lastFillPrice,
            int clientId,
            String whyHeld,
            double mktCapPrice,
            Instant timestamp
    ) {
        // Constructor that sets timestamp to current time
        public OrderStatus(
                int orderId,
                String status,
                BigDecimal filled,
                BigDecimal remaining,
                double avgFillPrice,
                long permId,
                int parentId,
                double lastFillPrice,
                int clientId,
                String whyHeld,
                double mktCapPrice
        ) {
            this(
                    orderId,
                    status,
                    filled,
                    remaining,
                    avgFillPrice,
                    permId,
                    parentId,
                    lastFillPrice,
                    clientId,
                    whyHeld,
                    mktCapPrice,
                    Instant.now()
            );
        }

        // id() property: returns orderId if > 0, else permId
        public long id() {
            return orderId > 0 ? orderId : permId;
        }
    }
}
