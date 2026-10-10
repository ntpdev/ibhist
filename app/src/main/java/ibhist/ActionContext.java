package ibhist;

import com.ib.client.EClientSocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport values owned by a live TWS connection. They are supplied per request by
 * {@link IBConnectorImpl} instead of being injected, because they only exist while a
 * connection is open.
 */
public record ActionContext(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue) {
}