package ibhist;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 * Wires the assisted injection factory that creates the per request TWS actions.
 * Each action class is both the factory return type and its own implementation,
 * so only the {@link ActionContext} and request details need to be passed in.
 */
public class ActionModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new FactoryModuleBuilder().build(ActionFactory.class));
        bind(EWrapperFactory.class).to(EWrapperFactoryImpl.class);
    }
}