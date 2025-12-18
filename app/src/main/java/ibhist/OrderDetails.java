package ibhist;

import com.ib.client.OrderType;
import com.ib.client.Types.Action;
import com.ib.client.Types.TimeInForce;

import java.util.ArrayList;
import java.util.List;


/**
 * OrderDetails is a simplified alternative to the IB Order class.
 * Use OrderBuilder to create IB orders from instances of this class.
 */
public class OrderDetails {
    // Enum to represent the action type for an order
    static final double TICK_SIZE = 0.25;
    static final double BASE = 6000.0;
    static final double LOWER = BASE * 0.95;
    static final double UPPER = BASE * 1.05;

    private final Action action;
    private final OrderType orderType;
    private final double price;
    private final int quantity;
    private final TimeInForce timeInForce;

    public OrderDetails(
            Action action,
            OrderType orderType,
            double price,
            int quantity,
            TimeInForce timeInForce
    ) {
        if (price < LOWER || price > UPPER) {
            throw new IllegalArgumentException("Price must be between " + LOWER + " and " + UPPER + ".");
        }
        if (quantity < 1 || quantity > 4) {
            throw new IllegalArgumentException("Quantity must be between 1 and 4.");
        }

        this.action = action;
        this.orderType = orderType;
        this.price = price;
        this.quantity = quantity;
        this.timeInForce = timeInForce;
    }

    public OrderDetails(
            Action action,
            double price,
            int quantity
    ) {
        this(action, OrderType.LMT, price, quantity, TimeInForce.DAY);
    }

    // Getters
    public Action getAction() {
        return action;
    }

    public Action getActionReversed() {
        return action == Action.BUY ? Action.SELL : Action.BUY;
    }


    public OrderType getOrderType() {
        return orderType;
    }

    public double getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    public TimeInForce getTimeInForce() {
        return timeInForce;
    }

    public double calculateLimitPrice(int offsetTicks) {
        double offset = asPrice(offsetTicks);
        return getAction() == Action.BUY
                ? getPrice() + offset
                : getPrice() - offset;
    }

    public double calculateStopPrice(int offsetTicks) {
        double offset = asPrice(offsetTicks);
        return getAction() == Action.BUY
                ? getPrice() - offset
                : getPrice() + offset;
    }

    private double asPrice(int offsetTicks) {
        if (offsetTicks < 4) {
            throw new IllegalArgumentException("Offset must be at least 4 ticks.");
        }
        return offsetTicks * TICK_SIZE;
    }

    public List<OrderDetails> createOrderGroup(int limitOffsetTicks, int stopOffsetTicks) {
        List<OrderDetails> orderGroup = new ArrayList<>();
        orderGroup.add(this);
        if (limitOffsetTicks > 0) {
            var limit = createProfitTakingOrder(limitOffsetTicks);
            orderGroup.add(limit);
        }
        if (stopOffsetTicks > 0) {
            var stop = createStopLossOrder(stopOffsetTicks);
            orderGroup.add(stop);
        }
        return orderGroup;
    }

    public OrderDetails createProfitTakingOrder(int offsetTicks) {
        return new OrderDetails(
                getActionReversed(),
                OrderType.LMT,
                calculateLimitPrice(offsetTicks),
                getQuantity(),
                getTimeInForce()
        );
    }

    public OrderDetails createStopLossOrder(int offsetTicks) {
        return new OrderDetails(
                getActionReversed(),
                OrderType.STP_LMT,
                calculateStopPrice(offsetTicks),
                getQuantity(),
                getTimeInForce()
        );
    }


    @Override
    public String toString() {
        return "OrderDetails{" +
                "action=" + action +
                ", orderType=" + orderType +
                ", price=" + price +
                ", quantity=" + quantity +
                ", timeInForce=" + timeInForce +
                '}';
    }
}