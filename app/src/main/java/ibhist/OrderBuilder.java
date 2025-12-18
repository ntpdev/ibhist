package ibhist;

import com.ib.client.Decimal;
import com.ib.client.Order;
import com.ib.client.OrderType;
import com.ib.client.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * OrderBuilder is a wrapper around the IB placeorder call. It takes OrderDetail objects converts
 * to IB orders and calls placeorder, handling oca groups and transmit flags.
 */
public class OrderBuilder {
    private static final Logger log = LogManager.getLogger(OrderBuilder.class.getSimpleName());
    public static final double TICK_SIZE = 0.25;

    final private AtomicInteger nextOrderId;
    final private Consumer<Order> placeOrder;

    public OrderBuilder(AtomicInteger nextOrderId, Consumer<Order> placeOrder) {
        this.nextOrderId = nextOrderId;
        this.placeOrder = Objects.requireNonNull(placeOrder);
    }

    /**
     * if list >1 first order is parent. if >2 then all apart from parent are in oca group
     * @param orderGroup - updates orderId, parentId, transmit and ocaGroup fields
     * @return parent order id
     */
    public int placeOrders(List<Order> orderGroup) {
        int parentId = 0;
        int countDown = orderGroup.size();
        String ocaLabel = orderGroup.size() > 2 ? ocaLabel() : "";
        for (var order : orderGroup) {
            if (order.orderId() != 0) {
                throw new IllegalArgumentException("orderId must be 0 for untransmitted orders");
            }
            order.orderId(nextOrderId.getAndIncrement());
            order.transmit(countDown == 1);
            if (parentId > 0) {
                order.parentId(parentId);
                if (!ocaLabel.isBlank()) {
                    order.ocaGroup(ocaLabel);
                    order.ocaType(Types.OcaType.CancelWithBlocking);
                }
            }

            placeOrder.accept(order);

            log.info("ib placeOrder " + order.orderId() + " " + order.action());
            if (parentId == 0) {
                parentId = order.orderId();
            }
            --countDown;
        }
        return parentId;
    }

    public static Order fromOrderDetails(OrderDetails details) {
        var order = new Order();
        order.action(details.getAction().name());
        order.orderType(details.getOrderType());
        if (details.getOrderType() == OrderType.LMT || details.getOrderType() == OrderType.STP_LMT) {
            order.lmtPrice(details.getPrice());
        }
        // auxPrice is the trigger price
        if (details.getOrderType() == OrderType.STP || details.getOrderType() == OrderType.STP_LMT) {
            order.auxPrice(details.getPrice());
        }
        order.totalQuantity(Decimal.get(details.getQuantity()));
        order.tif(details.getTimeInForce().name());
        return order;
    }

    public static List<Order> fromOrderDetails(List<OrderDetails> details) {
        return details.stream().map(OrderBuilder::fromOrderDetails).toList();
    }

    private static PriceHistory.Bar findLast(PriceHistory history, ArrayUtils.PointType type, int n) {
        var ix = history.indexEntry(-1);
        var swings = ArrayUtils.findSwings(history.getColumn("high"), history.getColumn("low"), ix.start(), ix.end(), n);
        for (int i = swings.size() - 1; i >= 0; --i) {
            var sw = swings.get(i);
            if (sw.type() == type) {
                return history.bar(sw.index());
            }
        }
        return null;
    }

    /**
     * Create a buy order group relative to most recent low
     * @param offset ticks relative to low. positive means above the low. negative means below the low.
     * @param limitOffset ticks relative to entry.
     * @param stopOffset ticks relative to the lower of entry or low.
     * @return an order group or empty list
     */
    public static List<OrderDetails> createBuyRelativeToLow(PriceHistory history, int offset, int limitOffset, int stopOffset) {
        if (limitOffset < 0 || stopOffset < 0) {
            throw new IllegalArgumentException("limitOffset and stopOffset must be positive");
        }
        var bar = findLast(history, ArrayUtils.PointType.LOCAL_LOW, 9);
        if (bar == null) {
            return Collections.emptyList();
        }
        var parent = new OrderDetails(Types.Action.BUY, bar.low() + TICK_SIZE * offset, 1);
        int stopOffsetFromEntry = (offset >= 0) ? offset + stopOffset : stopOffset;
        var orderGroup = parent.createOrderGroup(limitOffset, stopOffsetFromEntry);
        log.info(orderGroup);
        return orderGroup;
    }

    /**
     * Create a buy order group relative to most recent low
     * @param offset ticks relative to high. positive means below the high. negative means above the high.
     * @param limitOffset ticks relative to entry.
     * @param stopOffset ticks relative to the higher of entry or high.
     * @return an order group or empty list
     */
    public static List<OrderDetails> createSellRelativeToHigh(PriceHistory history, int offset, int limitOffset, int stopOffset) {
        if (limitOffset < 0 || stopOffset < 0) {
            throw new IllegalArgumentException("limitOffset and stopOffset must be positive");
        }
        var bar = findLast(history, ArrayUtils.PointType.LOCAL_HIGH, 9);
        if (bar == null) {
            return Collections.emptyList();
        }
        var parent = new OrderDetails(Types.Action.SELL, bar.high() - TICK_SIZE * offset, 1);
        int stopOffsetFromEntry = (offset >= 0) ? offset + stopOffset : stopOffset;
        var orderGroup = parent.createOrderGroup(limitOffset, stopOffsetFromEntry);
        log.info(orderGroup);
        return orderGroup;
    }

    /**
     * return a unique string of length 4 for oca group label
     */
    public static String ocaLabel() {
        long now = Instant.now().getNano();
        now >>= 16;
        // only encode last 6 bytes to avoid padding
        byte[] bytes = new byte[6];
        for (int i = 5; i >= 0; i--) {
            bytes[i] = (byte)(now & 0xff);
            now >>= 8;
        }
        String s = Base64.getEncoder().encodeToString(bytes);
        return s.substring(s.length() - 4);
    }

}
