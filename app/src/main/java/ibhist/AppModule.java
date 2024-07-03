package ibhist;

import com.google.inject.AbstractModule;

public class AppModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IBConnector.class).to(IBConnectorImpl.class);
        bind(ContractFactory.class).to(ContractFactoryImpl.class);
        bind(TimeSeriesRepository.class).to(TimeSeriesRepositoryImpl.class);
        bind(Repl.class).to(ReplImpl.class);
    }
}
