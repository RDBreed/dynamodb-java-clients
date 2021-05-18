package eu.luminis.breed.dynamodbmigration.user.repository;

import eu.luminis.breed.dynamodbmigration.user.model.User;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface UserRepository {
    User createOrUpdateUser(User user);

    Optional<User> getUserById(UUID id);

    List<User> findAll();

    List<User> findByIds(List<UUID> ids);

    List<User> findByLastName(String lastName);

    void updateUser(User user);

    void updateUserAdvanced(User user);

    void deleteUser(UUID id);
}
