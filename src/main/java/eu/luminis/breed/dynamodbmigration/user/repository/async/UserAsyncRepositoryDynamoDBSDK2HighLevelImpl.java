package eu.luminis.breed.dynamodbmigration.user.repository.async;

import eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb.User;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.net.URI;
import java.util.UUID;

import static eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb.UserMapper.MAPPER;

@Slf4j
public class UserAsyncRepositoryDynamoDBSDK2HighLevelImpl implements UserAsyncRepository {

    private final DynamoDbAsyncTable<User> userDynamoDbAsyncTable;
    private final DynamoDbAsyncIndex<User> userDynamoDbAsyncIndex;

    public UserAsyncRepositoryDynamoDBSDK2HighLevelImpl(String tableName) {
        var dynamoDbEnhancedAsyncClient = DynamoDbEnhancedAsyncClient.create();
        userDynamoDbAsyncTable = dynamoDbEnhancedAsyncClient.table(tableName, TableSchema.fromBean(User.class));
        userDynamoDbAsyncIndex = userDynamoDbAsyncTable.index("lastNameIndex");
    }

    public UserAsyncRepositoryDynamoDBSDK2HighLevelImpl(String tableName, String endpoint) {
        final DynamoDbAsyncClient dynamoDbClient = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .build();
        var dynamoDbEnhancedAsyncClient = DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
        userDynamoDbAsyncTable = dynamoDbEnhancedAsyncClient.table(tableName, TableSchema.fromBean(User.class));
        userDynamoDbAsyncIndex = userDynamoDbAsyncTable.index("lastNameIndex");
    }

    @Override
    public Mono<eu.luminis.breed.dynamodbmigration.user.model.User> createOrUpdateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        if (user == null) {
            throw UserException.errorIdIsNull();
        }
        if (user.getId() == null) {
            user.setId(UUID.randomUUID());
        }
        return Mono.fromFuture(userDynamoDbAsyncTable.updateItem(MAPPER.userToEnhancedUser(user))).map(MAPPER::enhancedUserToUser);
    }

    @Override
    public Mono<eu.luminis.breed.dynamodbmigration.user.model.User> getUserById(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromFuture(userDynamoDbAsyncTable.getItem(Key.builder().partitionValue(id.toString()).build()))
                .map(MAPPER::enhancedUserToUser);
    }

    @Override
    public Flux<eu.luminis.breed.dynamodbmigration.user.model.User> findAll() {
        return Flux.from(userDynamoDbAsyncTable.scan(ScanEnhancedRequest.builder().build()))
                .map(Page::items)
                .flatMapIterable(enhancedUsers -> enhancedUsers)
                .map(MAPPER::enhancedUserToUser);
    }

    @Override
    public Flux<eu.luminis.breed.dynamodbmigration.user.model.User> findByLastName(final String lastName) {
        return Flux.from(userDynamoDbAsyncIndex.query(QueryConditional
                .keyEqualTo(Key.builder()
                        .partitionValue(lastName).build())))
                .map(Page::items)
                .flatMapIterable(enhancedUsers -> enhancedUsers)
                .map(MAPPER::enhancedUserToUser);
    }

    @Override
    public Mono<Void> updateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        return Mono.fromFuture(userDynamoDbAsyncTable.updateItem(UpdateItemEnhancedRequest.builder(User.class)
                .item(MAPPER.userToEnhancedUser(user))
                .ignoreNulls(true).build()))
                .then();
    }

    @Override
    public Mono<Void> deleteUser(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromFuture(userDynamoDbAsyncTable.deleteItem(Key.builder().partitionValue(id.toString()).build()))
                .then();
    }
}
