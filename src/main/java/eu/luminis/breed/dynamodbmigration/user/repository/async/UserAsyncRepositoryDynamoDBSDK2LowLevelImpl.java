package eu.luminis.breed.dynamodbmigration.user.repository.async;

import eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v2.UserMapper;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.ID_FIELD;

@Slf4j
public class UserAsyncRepositoryDynamoDBSDK2LowLevelImpl implements UserAsyncRepository {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    public UserAsyncRepositoryDynamoDBSDK2LowLevelImpl(String tableName, String endpoint, String region, String key, String secret) {
        this.dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(key, secret)))
                .endpointOverride(URI.create(endpoint))
                .build();
        this.tableName = tableName;
    }

    @Override
    public Mono<User> createOrUpdateUser(User user) {
        if (user.getId() == null) {
            user.setId(UUID.randomUUID());
        }
        return Mono.fromFuture(dynamoDbAsyncClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(UserMapper.mapToItem(user, true))
                .build()))
                .thenReturn(user);
    }

    @Override
    public Mono<User> getUserById(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromFuture(dynamoDbAsyncClient.getItem(GetItemRequest.builder().tableName(tableName).key(Map.of(ID_FIELD,
                AttributeValue.builder().s(id.toString()).build())).build()))
                .map(UserMapper::mapToUser);
    }

    @Override
    public Flux<User> findAll() {
        return Mono.fromFuture(dynamoDbAsyncClient.scan(ScanRequest.builder().tableName(tableName).build()))
                .expand(scanResponse -> scanResponse.hasLastEvaluatedKey() ?
                        Mono.fromFuture(() -> dynamoDbAsyncClient.scan(ScanRequest.builder().tableName(tableName).exclusiveStartKey(scanResponse.lastEvaluatedKey()).build())) : Mono.empty())
                .flatMapIterable(ScanResponse::items)
                .map(UserMapper::mapToUser);
    }

    @Override
    public Flux<User> findByLastName(String lastName) {
        return Mono.fromFuture(dynamoDbAsyncClient.query(getQueryRequestBuilder(lastName).build()))
                .expand(response -> response.hasLastEvaluatedKey() ?
                        Mono.fromFuture(() -> dynamoDbAsyncClient.query(getQueryRequestBuilder(lastName).exclusiveStartKey(response.lastEvaluatedKey()).build())) :
                        Mono.empty())
                .flatMapIterable(QueryResponse::items)
                .map(UserMapper::mapToUser);
    }

    @Override
    public Mono<Void> updateUser(User user) {
        if (user.getId() == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromFuture(dynamoDbAsyncClient.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(ID_FIELD, AttributeValue.builder()
                        .s(user.getId().toString()).build()))
                .attributeUpdates(UserMapper.attributeValueUpdates(user))
                .build()))
                .then();
    }

    @Override
    public Mono<Void> deleteUser(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromFuture(dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(ID_FIELD, AttributeValue.builder().s(id.toString()).build()))
                .build()))
                .then();
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
