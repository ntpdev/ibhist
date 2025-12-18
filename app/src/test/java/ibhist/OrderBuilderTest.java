package ibhist;

import com.ib.client.Decimal;
import com.ib.client.Order;
import com.ib.client.OrderType;
import com.ib.client.Types;
import com.ib.client.Types.Action;
import com.ib.client.Types.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class OrderBuilderTest {

    private Consumer<Order> mockPlaceOrder;
    private OrderBuilder orderBuilder;
    private AtomicInteger nextOrderId;

    @BeforeEach
    void setUp() {
        mockPlaceOrder = mock(Consumer.class);
        nextOrderId = new AtomicInteger(1000);
        orderBuilder = new OrderBuilder(nextOrderId, mockPlaceOrder);
    }

    @Test
    void createOrderGroup_withParentOnly() {
        // Given
        var parent = new OrderDetails(
                Action.BUY,
                OrderType.LMT,
                6000.0,
                2,
                TimeInForce.DAY
        );

        // When
        var orders = parent.createOrderGroup(0, 0);

        // Then
        assertThat(orders).hasSize(1);
        var orderDetails = orders.getFirst();
        var order = OrderBuilder.fromOrderDetails(orderDetails);
        assertThat(order.action()).isEqualTo(Action.BUY);
        assertThat(order.orderType()).isEqualTo(OrderType.LMT);
        assertThat(order.lmtPrice()).isEqualTo(6000.0);
        assertThat(order.totalQuantity()).isEqualTo(Decimal.get(2));
        assertThat(order.tif()).isEqualTo(TimeInForce.DAY);
    }

    @Test
    void createOrderGroup_withLimitOffset() {
        // Given
        var parent = new OrderDetails(
                Action.BUY,
                OrderType.LMT,
                6000.0,
                2,
                TimeInForce.DAY
        );

        // When
        var orders = parent.createOrderGroup(32, 0);

        // Then
        assertThat(orders).hasSize(2);

        // Parent order
        Order parentOrder = OrderBuilder.fromOrderDetails(orders.getFirst());
        assertThat(parentOrder.action()).isEqualTo(Action.BUY);
        assertThat(parentOrder.lmtPrice()).isEqualTo(6000.0);

        // Profit taking order
        Order profitOrder = OrderBuilder.fromOrderDetails(orders.get(1));
        assertThat(profitOrder.action()).isEqualTo(Action.SELL);
        assertThat(profitOrder.orderType()).isEqualTo(OrderType.LMT);
        assertThat(profitOrder.lmtPrice()).isEqualTo(6008.0); // 6000 + (8 * 0.25)
    }

    @Test
    void createOrderGroup_withStopOffset() {
        // Given
        var parent = new OrderDetails(
                Action.BUY,
                OrderType.LMT,
                6000.0,
                2,
                TimeInForce.DAY
        );

        // When
        var orders = OrderBuilder.fromOrderDetails(parent.createOrderGroup( 0, 12));

        // Then
        assertThat(orders).hasSize(2);

        // Parent order
        Order parentOrder = orders.getFirst();
        assertThat(parentOrder.action()).isEqualTo(Action.BUY);

        // Stop loss order
        Order stopOrder = orders.get(1);
        assertThat(stopOrder.action()).isEqualTo(Action.SELL);
        assertThat(stopOrder.orderType()).isEqualTo(OrderType.STP_LMT);
        assertThat(stopOrder.auxPrice()).isEqualTo(5997.0); // 6000 - (12 * 0.25)
        assertThat(stopOrder.lmtPrice()).isEqualTo(5997.0); // 6000 - (12 * 0.25)
    }

    @Test
    void createOrderGroup_withBothOffsets() {
        // Given
        var parent = new OrderDetails(
                Action.SELL,
                OrderType.LMT,
                6000.0,
                1,
                TimeInForce.GTC
        );

        // When
        var orders = OrderBuilder.fromOrderDetails(parent.createOrderGroup( 4, 8));

        // Then
        assertThat(orders).hasSize(3);

        // Parent order
        Order parentOrder = orders.getFirst();
        assertThat(parentOrder.action()).isEqualTo(Action.SELL);

        // Profit taking order (reversed action)
        Order profitOrder = orders.get(1);
        assertThat(profitOrder.action()).isEqualTo(Action.BUY);
        assertThat(profitOrder.lmtPrice()).isEqualTo(5999.0); // 6000 - (4 * 0.25) for SELL

        // Stop loss order (reversed action)
        Order stopOrder = orders.get(2);
        assertThat(stopOrder.action()).isEqualTo(Action.BUY);
        assertThat(stopOrder.auxPrice()).isEqualTo(6002.0); // 6000 + (8 * 0.25) for SELL
        assertThat(stopOrder.lmtPrice()).isEqualTo(6002.0); // 6000 + (8 * 0.25) for SELL
    }

    @Test
    void placeOrders_singleOrder() {
        // Given
        Order order = new Order();
        var orders = List.of(order);

        // When
        int parentId = orderBuilder.placeOrders(orders);

        // Then
        assertThat(parentId).isEqualTo(1000);
        assertThat(order.orderId()).isEqualTo(1000);
        assertThat(order.transmit()).isTrue();
        assertThat(order.parentId()).isEqualTo(0);
        assertThat(order.ocaGroup()).isBlank();

        verify(mockPlaceOrder, times(1)).accept(order);
    }

    @Test
    void placeOrders_singleOrder_duplicate_rejected() {
        // Given
        Order order = new Order();
        var orders = List.of(order);

        // When
        orderBuilder.placeOrders(orders);

        // Then - second call should throw exception
        assertThatThrownBy(() -> orderBuilder.placeOrders(orders))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("orderId must be 0 for untransmitted orders");

        verify(mockPlaceOrder, times(1)).accept(order);
    }


    @Test
    void placeOrders_twoOrders() {
        // Given
        Order parentOrder = new Order();
        parentOrder.action(Action.BUY);
        Order childOrder = new Order();
        childOrder.action(Action.SELL);
        var orders = List.of(parentOrder, childOrder);

        // When
        int parentId = orderBuilder.placeOrders(orders);

        // Then
        assertThat(parentId).isEqualTo(1000);

        // Parent order
        assertThat(parentOrder.orderId()).isEqualTo(1000);
        assertThat(parentOrder.transmit()).isFalse();
        assertThat(parentOrder.parentId()).isEqualTo(0);
        assertThat(parentOrder.ocaGroup()).isBlank();

        // Child order
        assertThat(childOrder.orderId()).isEqualTo(1001);
        assertThat(childOrder.transmit()).isTrue();
        assertThat(childOrder.parentId()).isEqualTo(1000);
        assertThat(childOrder.ocaGroup()).isBlank();

        verify(mockPlaceOrder, times(2)).accept(any(Order.class));
    }

    @Test
    void placeOrders_threeOrdersWithOCA() {
        // Given
        Order parentOrder = new Order();
        parentOrder.action(Action.BUY);
        Order profitOrder = new Order();
        profitOrder.action(Action.SELL);
        Order stopOrder = new Order();
        stopOrder.action(Action.SELL);
        var orders = List.of(parentOrder, profitOrder, stopOrder);

        // When
        int parentId = orderBuilder.placeOrders(orders);

        // Then
        assertThat(parentId).isEqualTo(1000);

        // Parent order
        assertThat(parentOrder.parentId()).isEqualTo(0);
        assertThat(parentOrder.ocaGroup()).isBlank();

        // Child orders should have OCA group
        String ocaGroup = profitOrder.ocaGroup();
        assertThat(ocaGroup).isNotBlank();

        assertThat(profitOrder.parentId()).isEqualTo(1000);
        assertThat(profitOrder.ocaType()).isEqualTo(Types.OcaType.CancelWithBlocking);

        assertThat(stopOrder.ocaGroup()).isEqualTo(ocaGroup);
        assertThat(stopOrder.parentId()).isEqualTo(1000);
        assertThat(stopOrder.ocaType()).isEqualTo(Types.OcaType.CancelWithBlocking);

        // Verify orders were passed to mock in correct sequence
        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(mockPlaceOrder, times(3)).accept(orderCaptor.capture());
        var capturedOrders = orderCaptor.getAllValues();
        assertThat(capturedOrders).extracting(Order::orderId).containsExactly(1000, 1001, 1002);
        assertThat(capturedOrders).extracting(Order::transmit).containsExactly(false, false, true);
    }

    @Test
    void fromOrderDetails_allFields() {
        // Given
        OrderDetails details = new OrderDetails(
                Action.SELL,
                OrderType.STP_LMT,
                5950.0,
                3,
                TimeInForce.GTC
        );

        // When
        Order order = OrderBuilder.fromOrderDetails(details);

        // Then
        assertThat(order.action()).isEqualTo(Action.SELL);
        assertThat(order.orderType()).isEqualTo(OrderType.STP_LMT);
        assertThat(order.lmtPrice()).isEqualTo(5950.0);
        assertThat(order.totalQuantity()).isEqualTo(Decimal.get(3));
        assertThat(order.tif()).isEqualTo(TimeInForce.GTC);
    }

    @Test
    void createProfitTakingOrder_buyParent() {
        // Given
        var parent = new OrderDetails(
                Action.BUY,
                OrderType.LMT,
                6000.0,
                2,
                TimeInForce.DAY
        );

        // When
        OrderDetails profitOrder = parent.createProfitTakingOrder(8);

        // Then
        assertThat(profitOrder.getAction()).isEqualTo(Action.SELL);
        assertThat(profitOrder.getOrderType()).isEqualTo(OrderType.LMT);
        assertThat(profitOrder.getPrice()).isEqualTo(6002.0); // 6000 + (8 * 0.25)
        assertThat(profitOrder.getQuantity()).isEqualTo(2);
        assertThat(profitOrder.getTimeInForce()).isEqualTo(TimeInForce.DAY);
    }

    @Test
    void createProfitTakingOrder_sellParent() {
        // Given
        var parent = new OrderDetails(
                Action.SELL,
                OrderType.LMT,
                6000.0,
                1,
                TimeInForce.GTC
        );

        // When
        OrderDetails profitOrder = parent.createProfitTakingOrder(12);

        // Then
        assertThat(profitOrder.getAction()).isEqualTo(Action.BUY);
        assertThat(profitOrder.getOrderType()).isEqualTo(OrderType.LMT);
        assertThat(profitOrder.getPrice()).isEqualTo(5997.0); // 6000 - (12 * 0.25)
        assertThat(profitOrder.getQuantity()).isEqualTo(1);
        assertThat(profitOrder.getTimeInForce()).isEqualTo(TimeInForce.GTC);
    }

    @Test
    void createStopLossOrder_buyParent() {
        // Given
        var parent = new OrderDetails(
                Action.BUY,
                OrderType.LMT,
                6000.0,
                2,
                TimeInForce.DAY
        );

        // When
        OrderDetails stopOrder = parent.createStopLossOrder(16);

        // Then
        assertThat(stopOrder.getAction()).isEqualTo(Action.SELL);
        assertThat(stopOrder.getOrderType()).isEqualTo(OrderType.STP_LMT);
        assertThat(stopOrder.getPrice()).isEqualTo(5996.0); // 6000 - (16 * 0.25)
        assertThat(stopOrder.getQuantity()).isEqualTo(2);
        assertThat(stopOrder.getTimeInForce()).isEqualTo(TimeInForce.DAY);
    }

    @Test
    void createStopLossOrder_sellParent() {
        // Given
        var parent = new OrderDetails(
                Action.SELL,
                OrderType.LMT,
                6000.0,
                1,
                TimeInForce.GTC
        );

        // When
        OrderDetails stopOrder = parent.createStopLossOrder(20);

        // Then
        assertThat(stopOrder.getAction()).isEqualTo(Action.BUY);
        assertThat(stopOrder.getOrderType()).isEqualTo(OrderType.STP_LMT);
        assertThat(stopOrder.getPrice()).isEqualTo(6005.0); // 6000 + (20 * 0.25)
        assertThat(stopOrder.getQuantity()).isEqualTo(1);
        assertThat(stopOrder.getTimeInForce()).isEqualTo(TimeInForce.GTC);
    }

    @Test
    void ocaLabel_multipleCallsProduceDifferentResults() {
        // When
        String[] labels = new String[10];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = OrderBuilder.ocaLabel();
            // 1 ms delay to ensure different timestamps
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Then
        for (int i = 0; i < labels.length; i++) {
            assertThat(labels[i]).hasSize(4);
            for (int j = i + 1; j < labels.length; j++) {
                assertThat(labels[i]).isNotEqualTo(labels[j]);
            }
        }
    }
}