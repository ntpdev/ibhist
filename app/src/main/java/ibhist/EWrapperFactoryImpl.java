package ibhist;

public class EWrapperFactoryImpl implements EWrapperFactory {

    @Override
    public EWrapperImpl create(ActionProvider actionProvider) {
        return new EWrapperImpl(actionProvider);
    }
}