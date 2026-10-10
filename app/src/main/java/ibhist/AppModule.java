package ibhist;

import com.google.inject.AbstractModule;

public class AppModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new ActionModule());
        bind(IBConnector.class).to(IBConnectorImpl.class);
        bind(ContractFactory.class).to(ContractFactoryImpl.class);
        bind(TimeSeriesRepository.class).to(TimeSeriesRepositoryImpl.class);
        bind(PriceHistoryRepository.class).to(PriceHistoryRepositoryImpl.class);
        bind(Repl.class).to(ReplImpl.class);
        bind(EventService.class).to(EventServiceImpl.class).asEagerSingleton();
        bind(SoundPlayer.class).to(SoundPlayerImpl.class).asEagerSingleton();
        // single price monitor registry shared by the repl and the feed actions
        bind(MonitorManager.class).asEagerSingleton();
        // domain event listeners are registered while the container is created
        bind(EventListenerRegistrar.class).asEagerSingleton();
    }
}
