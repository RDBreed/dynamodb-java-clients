package eu.luminis.breed.dynamodbmigration.user.repository;

import eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v2.UserMapper;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.net.URI;
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
public class UserRepositoryDynamoDBSDK2LowLevelImpl implements UserRepository {

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public UserRepositoryDynamoDBSDK2LowLevelImpl(String tableName) {
        this.tableName = tableName;
        this.dynamoDbClient = DynamoDbClient.create();
    }

    public UserRepositoryDynamoDBSDK2LowLevelImpl(String tableName, String endpoint) {
        this.dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .build();
        this.tableName = tableName;
    }

    @Override
    public User createOrUpdateUser(User user) {
        if (user.getId() == null) {
            user.setId(UUID.randomUUID());
        }
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(UserMapper.mapToItem(user, true))
                .build());
        return user;
    }

    @Override
    public Optional<User> getUserById(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            final GetItemResponse item = dynamoDbClient.getItem(GetItemRequest.builder().tableName(tableName).key(Map.of(ID_FIELD,
                    AttributeValue.builder().s(id.toString()).build())).build());
            return Optional.ofNullable(UserMapper.mapToUser(item));
        } catch (Exception e) {
            log.error("Unable to retrieve data for id {}", id, e);
        }
        return Optional.empty();
    }

    @Override
    public List<User> findAll() {
        ScanResponse scan = dynamoDbClient.scan(ScanRequest.builder().tableName(tableName).build());
        final List<User> allUsers = scan.items().stream().map(UserMapper::mapToUser).collect(Collectors.toList());
        while (scan.hasLastEvaluatedKey()) {
            scan = dynamoDbClient.scan(ScanRequest.builder().tableName(tableName).exclusiveStartKey(scan.lastEvaluatedKey()).build());
//            scan.items().stream().map(UserMapper::mapToUser).forEach(allUsers::add);
        }
        return allUsers;
    }

    @Override
    public List<User> findByIds(List<UUID> ids) {
        //limiting to 100, because that is one of batchgetitem limits defined by AWS...
        final List<List<UUID>> chunks = ListUtils.partition(new ArrayList<>(ids), 100);
        return chunks.stream()
                .map(uuids -> dynamoDbClient.batchGetItem(BatchGetItemRequest.builder()
                        .requestItems(Map.of(tableName, KeysAndAttributes.builder()
                                .keys(uuids.stream()
                                        .map(id -> Map.of(ID_FIELD, AttributeValue.builder().s(String.valueOf(id)).build()))
                                        .collect(Collectors.toList()))
                                .build()))
                        .build()))
                .flatMap(batchGetItemResponse -> batchGetItemResponse.responses().get(tableName).stream().map(UserMapper::mapToUser))
                .collect(Collectors.toList());
    }

    @Override
    public List<User> findByLastName(final String lastName) {
        QueryResponse response = dynamoDbClient.query(getQueryRequestBuilder(lastName).build());
        final List<User> users = response.items().stream().map(UserMapper::mapToUser).collect(Collectors.toList());
        while (response.hasLastEvaluatedKey()) {
            response = dynamoDbClient.query(getQueryRequestBuilder(lastName).exclusiveStartKey(response.lastEvaluatedKey()).build());
            response.items().stream().map(UserMapper::mapToUser).forEach(users::add);
        }
        return users;
    }

    @Override
    public void updateUser(User user) {
        if (user.getId() == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            dynamoDbClient.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of(ID_FIELD, AttributeValue.builder()
                            .s(user.getId().toString()).build()))
                    .attributeUpdates(UserMapper.attributeValueUpdates(user))
                    .build());
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to update user with id {}", user.getId(), e);
        }
    }

    @Override
    public void updateUserAdvanced(User user) {
        if (user.getId() == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            final Map<String, AttributeValue> attributeValues = UserMapper.attributeValue(user);
            dynamoDbClient.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of(ID_FIELD, AttributeValue.builder()
                            .s(user.getId().toString()).build()))
                    .updateExpression(UserMapper.getUpdateExpression(user))
                    .expressionAttributeValues(attributeValues)
                    //deprecated
//                    .attributeUpdates(UserMapper.attributeValueUpdates(user))
                    .conditionExpression(LAST_MODIFIED_EXPRESSION)
                    .build());
        } catch (ConditionalCheckFailedException e) {
            throw UserException.error("User could not be updated as the update condition failed for user with id {}", user.getId());
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to update user with id {}", user.getId(), e);
        }
    }

    @Override
    public void deleteUser(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            dynamoDbClient.deleteItem(DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of(ID_FIELD, AttributeValue.builder().s(id.toString()).build()))
                    .build());
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to delete user with id {}", id, e);
        }
    }

    private QueryRequest.Builder getQueryRequestBuilder(String lastName) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(":lastName", AttributeValue.builder().s(lastName).build());
        return QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("lastName = :lastName")
                .indexName("lastNameIndex")
                .expressionAttributeValues(key);
    }


}
