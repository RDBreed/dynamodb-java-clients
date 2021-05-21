package eu.luminis.breed.dynamodbmigration.user.repository;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v1.UserMapper;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserNotUpdatedException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.ID_FIELD;
import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.LAST_MODIFIED_EXPRESSION;

@Slf4j
public class UserRepositoryDynamoDBSDK1LowLevelImpl implements UserRepository {

    private final AmazonDynamoDB amazonDynamoDBClient;
    private final String tableName;

    public UserRepositoryDynamoDBSDK1LowLevelImpl(String tableName) {
        this.tableName = tableName;
        this.amazonDynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    }

    public UserRepositoryDynamoDBSDK1LowLevelImpl(String tableName, String serviceEndpoint) {
        this.tableName = tableName;
        this.amazonDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .build();
    }

    @Override
    public User createOrUpdateUser(User user) {
        if (user == null) {
            throw UserException.errorIdIsNull();
        }
        if (user.getId() == null) {
            final var id = UUID.randomUUID();
            log.info("Creating user with id: {}", id);
            user.setId(id);
        }
        final var putItemRequest = new PutItemRequest(tableName, UserMapper.mapToItem(user, true));
        try {
            amazonDynamoDBClient.putItem(putItemRequest);
            return user;
        } catch (Exception e) {
            throw UserNotUpdatedException.error(user.getId(), e);
        }
    }

    @Override
    public Optional<User> getUserById(final UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        HashMap<String, AttributeValue> key = new HashMap<>();
        key.put(ID_FIELD, new AttributeValue(id.toString()));

        var request = new GetItemRequest(tableName, key);

        try {
            GetItemResult result = amazonDynamoDBClient.getItem(request);
            if (result != null && result.getItem() != null) {

                return Optional.of(UserMapper.mapToUser(result.getItem()));
            } else {
                log.warn("No matching user was found");
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data for id {}", id, e);
        }
        return Optional.empty();
    }

    @Override
    public List<User> findAll() {
        var scanResult = amazonDynamoDBClient.scan(new ScanRequest(tableName));
        final List<User> allUsers = scanResult.getItems().stream().map(UserMapper::mapToUser).collect(Collectors.toList());
        while (scanResult.getLastEvaluatedKey() != null) {
            scanResult = amazonDynamoDBClient.scan(new ScanRequest(tableName).withExclusiveStartKey(scanResult.getLastEvaluatedKey()));
            scanResult.getItems().stream().map(UserMapper::mapToUser).forEach(allUsers::add);
        }
        return allUsers;
    }

    @Override
    public List<User> findByIds(List<UUID> ids) {
        //limiting to 100, because that is one of batchgetitem limits defined by AWS...
        final List<List<UUID>> chunks = ListUtils.partition(new ArrayList<>(ids), 100);
        return chunks.stream()
                .map(uuids -> amazonDynamoDBClient.batchGetItem(
                        new BatchGetItemRequest(
                                Map.of(tableName,
                                        new KeysAndAttributes()
                                                .withKeys(uuids.stream()
                                                        .map(id -> Map.of(ID_FIELD, new AttributeValue(String.valueOf(id))))
                                                        .collect(Collectors.toList()))))))
                .flatMap(batchGetItemResult -> batchGetItemResult.getResponses().get(tableName)
                        .stream()
                        .map(UserMapper::mapToUser))
                .collect(Collectors.toList());
    }

    @Override
    public List<User> findByLastName(final String lastName) {
        var queryRequest = getLastNameIndexQuery(lastName);
        QueryResult result = amazonDynamoDBClient.query(queryRequest);
        final List<User> allUsers = result.getItems().stream().map(UserMapper::mapToUser).collect(Collectors.toList());
        while (result.getLastEvaluatedKey() != null) {
            result = amazonDynamoDBClient.query(getLastNameIndexQuery(lastName).withExclusiveStartKey(result.getLastEvaluatedKey()));
            result.getItems().stream().map(UserMapper::mapToUser).forEach(allUsers::add);
        }
        return allUsers;
    }

    @Override
    public void updateUser(final User user) {
        final var updateItemRequest = UserMapper.updateItemRequest(user, tableName);
        try {
            amazonDynamoDBClient.updateItem(updateItemRequest);
        } catch (Exception e) {
            throw UserNotUpdatedException.error(user.getId(), e);
        }
    }

    @Override
    public void updateUserAdvanced(final User user) {
        final var updateItemRequest = UserMapper.updateItemRequest(user, tableName);
        updateItemRequest.withConditionExpression(LAST_MODIFIED_EXPRESSION);
        try {
            amazonDynamoDBClient.updateItem(updateItemRequest);
        } catch (ConditionalCheckFailedException e) {
            throw UserNotUpdatedException.errorConditionCheck(user.getId());
        } catch (Exception e) {
            throw UserNotUpdatedException.error(user.getId(), e);
        }
    }

    @Override
    public void deleteUser(final UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            amazonDynamoDBClient.deleteItem(new DeleteItemRequest(tableName, Map.of(ID_FIELD, new AttributeValue(id.toString()))));
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to delete user with id {}", id, e);
        }
    }

    private QueryRequest getLastNameIndexQuery(String lastName) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(":lastName", new AttributeValue(lastName));
        return new QueryRequest(tableName)
                .withKeyConditionExpression("lastName = :lastName")
                .withIndexName("lastNameIndex")
                .withExpressionAttributeValues(key);
    }
}
