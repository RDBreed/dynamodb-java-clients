package eu.luminis.breed.dynamodbmigration.user.controller;

import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import eu.luminis.breed.dynamodbmigration.user.repository.async.UserAsyncRepository;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Random;
import java.util.UUID;

@RestController
@RequestMapping("/user/async")
public class UserAsyncController {

    private final List<UserAsyncRepository> userAsyncRepositories;

    public UserAsyncController(List<UserAsyncRepository> userAsyncRepositories) {
        this.userAsyncRepositories = userAsyncRepositories;
    }

    @PostMapping
    public Mono<User> createUser(@RequestBody User user) {
        if (user.getId() != null) {
            throw UserException.clientError("Invalid POST request: User id should be null, but was {}", user.getId());
        }
        return getRandomRepositoryImpl().createOrUpdateUser(user);
    }

    @GetMapping("/{id}")
    public Mono<User> getUser(@PathVariable("id") UUID id) {
        return getRandomRepositoryImpl().getUserById(id);
    }

    @GetMapping("/")
    public Flux<User> getUsers(@RequestParam(required = false) String lastName,
                               @RequestParam(required = false) List<UUID> ids) {
        if (lastName != null) {
            return getRandomRepositoryImpl().findByLastName(lastName);
        } else if (ids != null) {
            return getRandomRepositoryImpl().findByIds(ids);
        }
        return getRandomRepositoryImpl().findAll();
    }

    @PutMapping("/{id}")
    public Mono<User> updateUser(@RequestParam UUID id, @RequestBody User user) {
        if (user.getId() == null || !id.equals(user.getId())) {
            throw UserException.clientError("Invalid PUT request: User id in object should be same as pathparam id, but was {}", user.getId());
        }
        return getRandomRepositoryImpl().createOrUpdateUser(user);
    }

    @PatchMapping
    public Mono<Void> updateUserPartially(@RequestBody User user) {
        return getRandomRepositoryImpl().updateUser(user);
    }

    @DeleteMapping
    public Mono<Void> deleteUser(UUID id){
        return getRandomRepositoryImpl().deleteUser(id);
    }

    private UserAsyncRepository getRandomRepositoryImpl() {
        return userAsyncRepositories.get(new Random().nextInt(userAsyncRepositories.size()));
    }
}
