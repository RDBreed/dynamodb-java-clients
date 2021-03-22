package eu.luminis.breed.dynamodbmigration.user.repository.async;

import eu.luminis.breed.dynamodbmigration.user.model.User;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface UserAsyncRepository {
    Mono<User> createOrUpdateUser(User user);
    Mono<User> getUserById(UUID id);
    Flux<User> findAll();

    Flux<User> findByLastName(String lastName);

    Mono<Void> updateUser(User user);
    Mono<Void> deleteUser(UUID id);
}
