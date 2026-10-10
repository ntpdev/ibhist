package ibhist;

/**
 * Creates the TWS callback adapter. The connector passes itself as the
 * {@link ActionProvider} so callbacks are dispatched to the actions map of the
 * connector that issued the request.
 */
public interface EWrapperFactory {
    EWrapperImpl create(ActionProvider actionProvider);
}