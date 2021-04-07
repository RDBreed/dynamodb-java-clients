package eu.luminis.breed.dynamodbmigration.user.repository.async;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v1.UserMapper;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.ID_FIELD;

@Slf4j
public class UserAsyncRepositoryDynamoDBSDK1LowLevelImpl implements UserAsyncRepository {

    //There is a AmazonDynamoDBAsync client, but as it will give a java.util.concurrent.Future - which is not compatible - we just use a
    //sync client
    //here...
    private final AmazonDynamoDB amazonDynamoDBClient;
    private final String tableName;

    public UserAsyncRepositoryDynamoDBSDK1LowLevelImpl(String tableName) {
        this.tableName = tableName;
        this.amazonDynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    }

    public UserAsyncRepositoryDynamoDBSDK1LowLevelImpl(String tableName, String serviceEndpoint) {
        this.tableName = tableName;
        this.amazonDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .build();
    }

    @Override
    public Mono<User> createOrUpdateUser(User user) {
        if (user == null) {
            throw UserException.errorIdIsNull();
        }
        if (user.getId() == null) {
            final UUID id = UUID.randomUUID();
            log.info("Creating user with id: {}", id);
            user.setId(id);
        }
        return Mono.fromCallable(() -> amazonDynamoDBClient.putItem(new PutItemRequest(tableName, UserMapper.mapToItem(user, true))))
                .thenReturn(user);
    }

    @Override
    public Mono<User> getUserById(final UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        HashMap<String, AttributeValue> key = new HashMap<>();
        key.put(ID_FIELD, new AttributeValue().withS(id.toString()));

        return Mono.fromCallable(() -> amazonDynamoDBClient.getItem(new GetItemRequest(tableName, key)))
                .map(GetItemResult::getItem)
                .map(UserMapper::mapToUser);
    }

    @Override
    public Flux<User> findAll() {
        return Mono.fromCallable(() -> amazonDynamoDBClient.scan(new ScanRequest(tableName)))
                .expand(scanResult -> scanResult.getLastEvaluatedKey() != null ?
                        Mono.fromCallable(() -> amazonDynamoDBClient.scan(new ScanRequest(tableName).withExclusiveStartKey(scanResult.getLastEvaluatedKey())))
                        : Mono.empty())
                .flatMapIterable(ScanResult::getItems)
                .map(UserMapper::mapToUser);
    }

    @Override
    public Flux<User> findByLastName(final String lastName) {
        return Mono.fromCallable(() -> amazonDynamoDBClient.query(getLastNameIndexQuery(lastName)))
                .expand(queryResult -> queryResult.getLastEvaluatedKey() != null ?
                        Mono.fromCallable(() -> amazonDynamoDBClient.query(getLastNameIndexQuery(lastName).withExclusiveStartKey(queryResult.getLastEvaluatedKey())))
                        : Mono.empty())
                .flatMapIterable(QueryResult::getItems)
                .map(UserMapper::mapToUser);
    }

    @Override
    public Mono<Void> updateUser(final User user) {
        return Mono.fromCallable(() -> amazonDynamoDBClient.updateItem(UserMapper.updateItemRequest(user, tableName))).then();
    }

    @Override
    public Mono<Void> deleteUser(final UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromCallable(() -> amazonDynamoDBClient.deleteItem(new DeleteItemRequest(tableName, Map.of(ID_FIELD, new AttributeValue(id.toString()))))).then();
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
