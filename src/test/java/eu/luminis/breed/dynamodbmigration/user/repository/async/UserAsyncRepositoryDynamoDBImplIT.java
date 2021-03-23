package eu.luminis.breed.dynamodbmigration.user.repository.async;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import eu.luminis.breed.dynamodbmigration.TestUtil;
import eu.luminis.breed.dynamodbmigration.user.model.Address;
import eu.luminis.breed.dynamodbmigration.user.model.Education;
import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepository;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK1HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK1LowLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK2HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK2LowLevelImpl;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.*;
import static eu.luminis.breed.dynamodbmigration.user.model.Gender.FEMALE;
import static org.assertj.core.api.Assertions.assertThat;


class UserAsyncRepositoryDynamoDBImplIT extends TestUtil {

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldCreateOrUpdateUser(UserAsyncRepository userRepository) {
        StepVerifier
                .create(userRepository.createOrUpdateUser(User.builder()
                        .firstName("firstName")
                        .age(21)
                        .gender(FEMALE)
                        .isAdmin(false)
                        .education(Education.builder().primarySchool(Address.builder().number(1).street("Schoolstreet").zipCode("4444").build()).build())
                        .address(Address.builder().street("Mainstreet").build()).build()))
                .assertNext(user ->  assertThat(user).isNotNull()
                        .extracting(
                                User::getFirstName,
                                User::getAge,
                                User::getGender,
                                User::getIsAdmin,
                                user1 -> user1.getAddress().getStreet())
                        .contains(
                                "firstName",
                                21,
                                FEMALE,
                                false,
                                "Mainstreet"))
                .verifyComplete();
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldGetUserById(UserAsyncRepository userRepository) {
        final UUID id = createUser("firstName", "lastName");
        StepVerifier
                .create(userRepository.getUserById(id))
                .assertNext(user ->  assertThat(user).isNotNull()
                        .extracting(
                                User::getId)
                .isEqualTo(id))
                .verifyComplete();
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldFindAll(UserAsyncRepository userRepository) {
        final String firstName = UUID.randomUUID().toString();
        for (int i = 0; i < 50; i++) {
            createUser(firstName, "lastname");
        }
        StepVerifier
                .create(userRepository.findAll())
                .recordWith(ArrayList::new)
                .thenConsumeWhile(x -> true)
                .consumeRecordedWith(users -> {
                    assertThat(users.stream()
                            .filter(user -> firstName.equals(user.getFirstName()))).hasSize(50);
                })
                .verifyComplete();
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldFindByLastName(UserAsyncRepository userRepository) {
        final String lastName = UUID.randomUUID().toString();
        for (int i = 0; i < 50; i++) {
            createUser("firstName", lastName);
        }
        StepVerifier
                .create(userRepository.findByLastName(lastName))
                .recordWith(ArrayList::new)
                .expectNextCount(50)
                .verifyComplete();
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldUpdateUser(UserAsyncRepository userRepository) {
        final UUID id = createUser("first", "last");
        StepVerifier
                .create(userRepository.updateUser(User.builder().id(id).firstName("second").build()))
                .expectNextCount(0)
                .verifyComplete();

        final GetItemResult user = amazonDynamoDBClient.getItem(new GetItemRequest(tableName, Map.of("id", new AttributeValue(id.toString()))));
        assertThat(user.getItem()).extracting(
                stringAttributeValueMap -> stringAttributeValueMap.get(FIRST_NAME_FIELD).getS(),
                stringAttributeValueMap -> stringAttributeValueMap.get(LAST_NAME_FIELD).getS())
                .containsExactly("second", "last");
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldDeleteUser(UserAsyncRepository userRepository) {
        final UUID id = createUser();
        StepVerifier
                .create(userRepository.deleteUser(id))
                .expectNextCount(0)
                .verifyComplete();
        final GetItemResult user = amazonDynamoDBClient.getItem(new GetItemRequest(tableName, Map.of("id", new AttributeValue(id.toString()))));
        assertThat(user.getItem()).isNull();
    }

    private UUID createUser() {
        return createUser(null, null);
    }

    private UUID createUser(String firstName, String lastName) {
        final UUID id = UUID.randomUUID();
        final Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        attributeValueMap.put(ID_FIELD, new AttributeValue(id.toString()));
        if (firstName != null) {
            attributeValueMap.put(FIRST_NAME_FIELD, new AttributeValue(firstName));
        }
        if (lastName != null) {
            attributeValueMap.put(LAST_NAME_FIELD, new AttributeValue(lastName));
        }
        attributeValueMap.put(ADDRESS_FIELD + "." + ADDRESS_STREET_FIELD, new AttributeValue("Mainstreet"));
        attributeValueMap.put(ADDRESS_FIELD + "." + ADDRESS_NUMBER_FIELD, new AttributeValue().withN("1"));
        attributeValueMap.put(ADDRESS_FIELD + "." + ADDRESS_ZIPCODE_FIELD, new AttributeValue("1111 AA"));
        attributeValueMap.put(IS_ADMIN_FIELD, new AttributeValue().withBOOL(true));
        amazonDynamoDBClient.putItem(tableName, attributeValueMap);
        return id;
    }

    static class RepositoryProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    new UserAsyncRepositoryDynamoDBSDK1LowLevelImpl(tableName, endpoint, region, key, secret),
                    new UserAsyncRepositoryDynamoDBSDK1HighLevelImpl(tableName, endpoint, region, key, secret),
                    new UserAsyncRepositoryDynamoDBSDK2LowLevelImpl(tableName, endpoint, region, key, secret),
                    new UserAsyncRepositoryDynamoDBSDK2HighLevelImpl(tableName, endpoint, region, key, secret)
            ).map(Arguments::of);
        }
    }
}



