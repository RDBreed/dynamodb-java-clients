package eu.luminis.breed.dynamodbmigration.user.controller;

import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserNotFoundException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Random;
import java.util.UUID;

@RestController
@RequestMapping("/user")
public class UserController {

    private final List<UserRepository> userRepositories;

    public UserController(List<UserRepository> userRepositories) {
        this.userRepositories = userRepositories;
    }

    @PostMapping
    public User createUser(@RequestBody User user) {
        if (user.getId() != null) {
            throw UserException.error("Invalid POST request: User id should be null, but was {}", user.getId());
        }
        return getRandomRepositoryImpl().createOrUpdateUser(user);
    }

    @GetMapping("/{id}")
    public User getUser(@PathVariable("id") UUID id) {
        return getRandomRepositoryImpl().getUserById(id).orElseThrow(UserNotFoundException::new);
    }

    @GetMapping("/")
    public List<User> getUsers(@RequestParam(required = false) String lastName) {
        if (lastName != null) {
            return getRandomRepositoryImpl().findByLastName(lastName);
        }
        return getRandomRepositoryImpl().findAll();
    }

    @PutMapping("/{id}")
    public User updateUser(@RequestParam UUID id, @RequestBody User user) {
        if (user.getId() == null || !id.equals(user.getId())) {
            throw UserException.error("Invalid PUT request: User id in object should be same as pathparam id, but was {}", user.getId());
        }
        return getRandomRepositoryImpl().createOrUpdateUser(user);
    }

    @PatchMapping
    public void updateUserPartially(@RequestBody User user) {
        getRandomRepositoryImpl().updateUser(user);
    }

    private UserRepository getRandomRepositoryImpl() {
        return userRepositories.get(new Random().nextInt(userRepositories.size()));
    }
}
