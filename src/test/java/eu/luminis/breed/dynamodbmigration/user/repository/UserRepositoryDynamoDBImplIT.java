package eu.luminis.breed.dynamodbmigration.user.repository;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import eu.luminis.breed.dynamodbmigration.TestUtil;
import eu.luminis.breed.dynamodbmigration.user.model.Address;
import eu.luminis.breed.dynamodbmigration.user.model.Education;
import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.*;
import static org.assertj.core.api.Assertions.assertThat;


class UserRepositoryDynamoDBImplIT extends TestUtil {

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldCreateOrUpdateUser(UserRepository userRepository) {
        final User user = userRepository.createOrUpdateUser(User.builder()
                .firstName("firstName")
                .age(21)
                .gender(Gender.FEMALE)
                .isAdmin(false)
                .education(Education.builder().primarySchool(Address.builder().number(1).street("Schoolstreet").zipCode("4444").build()).build())
                .address(Address.builder().street("Mainstreet").build()).build());
        final GetItemResult item = amazonDynamoDBClient.getItem(new GetItemRequest(tableName, Map.of("id",
                new AttributeValue(user.getId().toString()))));
        final Map<String, AttributeValue> itemMap = item.getItem();
        assertThat(itemMap).isNotNull()
                .extracting(
                        getValue(ID_FIELD, AttributeValue::getS),
                        getValue(FIRST_NAME_FIELD, AttributeValue::getS),
                        getValue(AGE_FIELD, AttributeValue::getN),
                        getValue(GENDER_FIELD, AttributeValue::getS),
                        getValue(IS_ADMIN_FIELD, AttributeValue::getBOOL),
                        getValue(ADDRESS_FIELD, value -> value.getM().get(ADDRESS_STREET_FIELD)))
                .contains(
                        user.getId().toString(),
                        "firstName",
                        "21",
                        "FEMALE",
                        false,
                        new AttributeValue("Mainstreet"));
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldGetUserById(UserRepository userRepository) {
        final UUID id = createUser("firstName", "lastName");
        final Optional<User> userById = userRepository.getUserById(id);
        assertThat(userById).isNotEmpty();
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldFindAll(UserRepository userRepository) {
        final String firstName = UUID.randomUUID().toString();
        for (int i = 0; i < 25; i++) {
            createUser(firstName, "lastname");
        }
        final List<User> users = userRepository.findAll();
        //have to filter on specific value so that we do not have interference with other results
        assertThat(users.stream().filter(user -> firstName.equals(user.getFirstName()))).hasSize(25);
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldFindByLastName(UserRepository userRepository) {
        final String lastName = UUID.randomUUID().toString();
        for (int i = 0; i < 50; i++) {
            createUser("firstName", lastName);
        }
        final List<User> users = userRepository.findByLastName(lastName);
        assertThat(users).hasSize(50);
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldUpdateUser(UserRepository userRepository) {
        final UUID id = createUser("first", "last");
        userRepository.updateUser(User.builder().id(id).firstName("second").build());
        final GetItemResult user = amazonDynamoDBClient.getItem(new GetItemRequest(tableName, Map.of("id", new AttributeValue(id.toString()))));
        assertThat(user.getItem()).extracting(
                stringAttributeValueMap -> stringAttributeValueMap.get(FIRST_NAME_FIELD).getS(),
                stringAttributeValueMap -> stringAttributeValueMap.get(LAST_NAME_FIELD).getS())
                .containsExactly("second", "last");
    }

    @ParameterizedTest
    @ArgumentsSource(RepositoryProvider.class)
    void shouldDeleteUser(UserRepository userRepository) {
        final UUID id = createUser();
        userRepository.deleteUser(id);
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

    private Function<Map<String, AttributeValue>, Object> getValue(String fieldName, Function<AttributeValue, Object> attributeGetter) {
        return stringAttributeValueMap -> {
            final AttributeValue attributeValue = stringAttributeValueMap.get(fieldName);
            return attributeGetter.apply(attributeValue);
        };
    }

    static class RepositoryProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    new UserRepositoryDynamoDBSDK1LowLevelImpl(tableName, endpoint, region, key, secret),
                    new UserRepositoryDynamoDBSDK1HighLevelImpl(tableName, endpoint, region, key, secret),
                    new UserRepositoryDynamoDBSDK2LowLevelImpl(tableName, endpoint, region, key, secret),
                    new UserRepositoryDynamoDBSDK2HighLevelImpl(tableName, endpoint, region, key, secret)
            ).map(Arguments::of);
        }
    }
}



