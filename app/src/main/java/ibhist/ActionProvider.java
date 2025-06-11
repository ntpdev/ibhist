package ibhist;

import java.util.Optional;

public interface ActionProvider {
    <T extends Action> Optional<T> findByType(Class<T> clz);
    Action findById(int id);
    String actionsToString();
}
