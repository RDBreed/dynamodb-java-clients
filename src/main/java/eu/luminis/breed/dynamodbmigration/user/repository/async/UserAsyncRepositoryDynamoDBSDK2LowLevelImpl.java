package eu.luminis.breed.dynamodbmigration.user.repository.async;

import eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v2.UserMapper;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.paginators.BatchGetItemPublisher;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.ID_FIELD;

@Slf4j
public class UserAsyncRepositoryDynamoDBSDK2LowLevelImpl implements UserAsyncRepository {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    public UserAsyncRepositoryDynamoDBSDK2LowLevelImpl(String tableName) {
        this.dynamoDbAsyncClient = DynamoDbAsyncClient.create();
        this.tableName = tableName;
    }

    public UserAsyncRepositoryDynamoDBSDK2LowLevelImpl(String tableName, String endpoint) {
        this.dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
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
    public Flux<User> findByIds(List<UUID> ids) {
        //limiting to 100, because that is one of batchgetitem limits defined by AWS...
        final List<List<UUID>> chunks = ListUtils.partition(new ArrayList<>(ids), 100);
        //displaying multiple ways that lead to the same
        return getRandomMethod(chunks);
    }

    private Flux<User> getRandomMethod(final List<List<UUID>> chunks) {
        List<Function<List<List<UUID>>, Flux<User>>> methods = new ArrayList<>();
        methods.add(this::approachOne);
        methods.add(this::approachTwo);
        return methods.get(new Random().nextInt(methods.size())).apply(chunks);
    }

    private Flux<User> approachOne(final List<List<UUID>> chunks) {
        return Flux.fromIterable(chunks)
                .map(uuids -> dynamoDbAsyncClient.batchGetItem(BatchGetItemRequest.builder()
                        .requestItems(Map.of(tableName, KeysAndAttributes.builder()
                                .keys(uuids.stream()
                                        .map(id -> Map.of(ID_FIELD, AttributeValue.builder().s(String.valueOf(id)).build()))
                                        .collect(Collectors.toList()))
                                .build()))
                        .build()))
                .flatMap(Mono::fromFuture)
                .flatMap(batchGetItemResponse -> Flux.fromStream(batchGetItemResponse.responses().get(tableName).stream().map(UserMapper::mapToUser)));

    }

    private Flux<User> approachTwo(final List<List<UUID>> chunks) {
        final List<BatchGetItemPublisher> batchGetItemPublishers = chunks
                .stream()
                .map(uuids -> dynamoDbAsyncClient.batchGetItemPaginator(BatchGetItemRequest.builder()
                        .requestItems(Map.of(tableName, KeysAndAttributes.builder()
                                .keys(uuids.stream()
                                        .map(id -> Map.of(ID_FIELD, AttributeValue.builder().s(String.valueOf(id)).build()))
                                        .collect(Collectors.toList()))
                                .build()))
                        .build()))
                .collect(Collectors.toList());
        return Flux.merge(batchGetItemPublishers)
                .flatMap(batchGetItemResponse -> Flux.fromStream(batchGetItemResponse.responses().get(tableName).stream().map(UserMapper::mapToUser)));
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
