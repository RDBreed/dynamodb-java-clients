package eu.luminis.breed.dynamodbmigration.user.repository.async;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper.User;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper.UserMapper.MAPPER;

@Slf4j
public class UserAsyncRepositoryDynamoDBSDK1HighLevelImpl implements UserAsyncRepository {

    private final DynamoDBMapper dynamoDBMapper;
    private final String tableName;

    public UserAsyncRepositoryDynamoDBSDK1HighLevelImpl(String tableName) {
        final var amazonDynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.tableName = tableName;
        this.dynamoDBMapper = new DynamoDBMapper(amazonDynamoDBClient, DynamoDBMapperConfig.builder()
                .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(this.tableName))
                .build());
    }

    public UserAsyncRepositoryDynamoDBSDK1HighLevelImpl(String tableName, String serviceEndpoint) {
        final var amazonDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .build();
        this.tableName = tableName;
        this.dynamoDBMapper = new DynamoDBMapper(amazonDynamoDBClient, DynamoDBMapperConfig.builder()
                .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(this.tableName))
                .build());
    }


    @Override
    public Mono<eu.luminis.breed.dynamodbmigration.user.model.User> createOrUpdateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        if (user == null) {
            throw UserException.errorIdIsNull();
        }
        if (user.getId() == null) {
            user.setId(UUID.randomUUID());
        }
        final var userMapper = MAPPER.userToMapperUser(user);
        return Mono.fromRunnable(() -> dynamoDBMapper.save(userMapper))
                .thenReturn(MAPPER.mapperUserToUser(userMapper));
    }

    @Override
    public Mono<eu.luminis.breed.dynamodbmigration.user.model.User> getUserById(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }

        return Mono.fromCallable(() -> dynamoDBMapper.load(User.class, id))
                .map(MAPPER::mapperUserToUser);
    }

    @Override
    public Flux<eu.luminis.breed.dynamodbmigration.user.model.User> findAll() {
        return Flux.fromIterable(dynamoDBMapper.scan(User.class, new DynamoDBScanExpression()))
                .map(MAPPER::mapperUserToUser);
    }

    @Override
    public Flux<eu.luminis.breed.dynamodbmigration.user.model.User> findByLastName(final String lastName) {
        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName("lastNameIndex")
                .withConsistentRead(false)
                .withHashKeyValues(User.builder().lastName(lastName)
                        .build());
        return Flux.fromIterable(dynamoDBMapper.query(User.class, queryExpression))
                .map(MAPPER::mapperUserToUser);
    }

    @Override
    public Mono<Void> updateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        return Mono.fromRunnable(() -> dynamoDBMapper.save(MAPPER.userToMapperUser(user),
                DynamoDBMapperConfig.builder()
                        .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(this.tableName))
                        .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES).build()))
                .then();
    }

    @Override
    public Mono<Void> deleteUser(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        return Mono.fromRunnable(() -> dynamoDBMapper.delete(User.builder().id(id).build()))
                .then();
    }
}
