package eu.luminis.breed.dynamodbmigration.user.repository;

import eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb.User;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserNotUpdatedException;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.LAST_MODIFIED_EXPRESSION;
import static eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb.UserMapper.MAPPER;

@Slf4j
public class UserRepositoryDynamoDBSDK2HighLevelImpl implements UserRepository {

    private final DynamoDbTable<User> userDynamoDbTable;
    private final DynamoDbIndex<User> userDynamoDbIndex;
    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;

    public UserRepositoryDynamoDBSDK2HighLevelImpl(String tableName) {
        dynamoDbEnhancedClient = DynamoDbEnhancedClient.create();
        userDynamoDbTable = dynamoDbEnhancedClient.table(tableName,
                TableSchema.fromBean(User.class));
        userDynamoDbIndex = userDynamoDbTable.index("lastNameIndex");
    }

    public UserRepositoryDynamoDBSDK2HighLevelImpl(String tableName, String endpoint) {
        final var dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .build();
        dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
        userDynamoDbTable = dynamoDbEnhancedClient.table(tableName,
                TableSchema.fromBean(User.class));
        userDynamoDbIndex = userDynamoDbTable.index("lastNameIndex");
    }

    @Override
    public eu.luminis.breed.dynamodbmigration.user.model.User createOrUpdateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        if (user == null) {
            throw UserException.errorIdIsNull();
        }
        if (user.getId() == null) {
            user.setId(UUID.randomUUID());
        }
        try {
            return MAPPER.enhancedUserToUser(userDynamoDbTable.updateItem(MAPPER.userToEnhancedUser(user)));
        } catch (Exception e) {
            throw UserNotUpdatedException.error(user.getId(), e);
        }
    }

    @Override
    public Optional<eu.luminis.breed.dynamodbmigration.user.model.User> getUserById(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            final var item = userDynamoDbTable.getItem(Key.builder().partitionValue(id.toString()).build());
            return Optional.ofNullable(item).map(MAPPER::enhancedUserToUser);
        } catch (Exception e) {
            log.error("Unable to retrieve data for id {}", id, e);
        }
        return Optional.empty();
    }

    @Override
    public List<eu.luminis.breed.dynamodbmigration.user.model.User> findAll() {
        final PageIterable<User> scan = userDynamoDbTable.scan(ScanEnhancedRequest.builder().build());
        return scan.stream()
                .flatMap(p -> p.items()
                        .stream())
                .map(MAPPER::enhancedUserToUser)
                .collect(Collectors.toList());
    }

    @Override
    public List<eu.luminis.breed.dynamodbmigration.user.model.User> findByIds(List<UUID> ids) {
        final ReadBatch.Builder<User> readBatchBuilder = ReadBatch.builder(User.class)
                .mappedTableResource(userDynamoDbTable);
        ids
                .stream()
                .map(uuid -> Key.builder()
                        .partitionValue(uuid.toString())
                        .build())
                .forEach(readBatchBuilder::addGetItem);
        return dynamoDbEnhancedClient.batchGetItem(BatchGetItemEnhancedRequest.builder()
                .addReadBatch(readBatchBuilder.build()).build())
                .stream()
                .flatMap(p -> p.resultsForTable(userDynamoDbTable)
                        .stream())
                .map(MAPPER::enhancedUserToUser)
                .collect(Collectors.toList());
    }

    @Override
    public List<eu.luminis.breed.dynamodbmigration.user.model.User> findByLastName(final String lastName) {
        return userDynamoDbIndex.query(QueryConditional
                .keyEqualTo(Key.builder()
                        .partitionValue(lastName).build()))
                .stream()
                .flatMap(p -> p.items()
                        .stream())
                .map(MAPPER::enhancedUserToUser)
                .collect(Collectors.toList());
    }

    @Override
    public void updateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        try {
            userDynamoDbTable.updateItem(UpdateItemEnhancedRequest.builder(User.class)
                    .item(MAPPER.userToEnhancedUser(user))
                    .ignoreNulls(true)
                    .build());
        } catch (Exception e) {
            throw UserNotUpdatedException.error(user.getId(), e);
        }
    }

    @Override
    public void updateUserAdvanced(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        try {
            final var updatedUser = MAPPER.userToEnhancedUser(user);
            userDynamoDbTable.updateItem(UpdateItemEnhancedRequest.builder(User.class)
                    .item(updatedUser)
                    .conditionExpression(Expression.builder()
                            .expression(LAST_MODIFIED_EXPRESSION)
                            .putExpressionValue(":lastModified", AttributeValue.builder().s(updatedUser.getLastModified().toString()).build())
                            .build())
                    .ignoreNulls(true)
                    .build());
        } catch (ConditionalCheckFailedException e) {
            throw UserNotUpdatedException.errorConditionCheck(user.getId());
        } catch (Exception e) {
            throw UserNotUpdatedException.error(user.getId(), e);
        }
    }

    @Override
    public void deleteUser(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            userDynamoDbTable.deleteItem(Key.builder().partitionValue(id.toString()).build());
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to delete user with id {}", id, e);
        }

    }
}
