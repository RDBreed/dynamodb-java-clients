package eu.luminis.breed.dynamodbmigration.user.repository;

import eu.luminis.breed.dynamodbmigration.user.model.User;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

public interface UserRepository {
    User createOrUpdateUser(User user);
    Optional<User> getUserById(UUID id);
    List<User> findAll();
    List<User> findByLastName(String lastName);
    void updateUser(User user);
    void deleteUser(UUID id);

    /**
     * Safely perform action. If value is null, action is not performed.
     * @param value to use in consumer.
     * @param putInItemAction the action to perform.
     * @param <T> the type of the value to use in the consumer.
     */
    public static <T> void putInItem(T value, Consumer<T> putInItemAction) {
        if (value != null) {
            putInItemAction.accept(value);
        }
    }
}
