package eu.luminis.breed.dynamodbmigration.user.repository;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.KeyPair;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper.User;
import eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v1.UserMapper;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.LAST_MODIFIED_EXPRESSION;
import static eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper.UserMapper.MAPPER;

@Slf4j
public class UserRepositoryDynamoDBSDK1HighLevelImpl implements UserRepository {

    private final DynamoDBMapper dynamoDBMapper;
    private final String tableName;
    private AmazonDynamoDB amazonDynamoDB;

    public UserRepositoryDynamoDBSDK1HighLevelImpl(String tableName) {
        this.tableName = tableName;
        this.amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        this.dynamoDBMapper = new DynamoDBMapper(
                amazonDynamoDB,
                DynamoDBMapperConfig
                        .builder()
                        .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(this.tableName))
                        .build());
    }

    public UserRepositoryDynamoDBSDK1HighLevelImpl(String tableName, String serviceEndpoint) {
        this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .build();
        this.tableName = tableName;
        this.dynamoDBMapper = new DynamoDBMapper(
                amazonDynamoDB,
                DynamoDBMapperConfig
                        .builder()
                        .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(this.tableName))
                        .build());
    }


    @Override
    public eu.luminis.breed.dynamodbmigration.user.model.User createOrUpdateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        if (user == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            final User userMapper = MAPPER.userToMapperUser(user);
            dynamoDBMapper.save(userMapper);
            return MAPPER.mapperUserToUser(userMapper);
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to update user with id {}", user.getId(), e);
        }
    }

    @Override
    public Optional<eu.luminis.breed.dynamodbmigration.user.model.User> getUserById(UUID id) {
        if (id == null) {
            throw UserException.errorIdIsNull();
        }
        try {
            final User userMapper = dynamoDBMapper.load(User.class, id);
            return Optional.ofNullable(userMapper).map(MAPPER::mapperUserToUser);
        } catch (Exception e) {
            log.error("Unable to retrieve data for id {}", id, e);
        }
        return Optional.empty();
    }

    @Override
    public List<eu.luminis.breed.dynamodbmigration.user.model.User> findAll() {
        return dynamoDBMapper.scan(User.class, new DynamoDBScanExpression())
                .stream()
                .map(MAPPER::mapperUserToUser)
                .collect(Collectors.toList());
    }

    @Override
    public List<eu.luminis.breed.dynamodbmigration.user.model.User> findByIds(List<UUID> ids) {
        final List<KeyPair> keyPairs = ids.stream().map(id -> new KeyPair().withHashKey(id)).collect(Collectors.toList());
        final List<Object> usersMapper = dynamoDBMapper.batchLoad(Map.of(User.class, keyPairs)).get(tableName);
        return usersMapper
                .stream()
                .map((Object user) -> MAPPER.mapperUserToUser((User) user))
                .collect(Collectors.toList());
    }

    @Override
    public List<eu.luminis.breed.dynamodbmigration.user.model.User> findByLastName(final String lastName) {
        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName("lastNameIndex")
                .withConsistentRead(false)
                .withHashKeyValues(User.builder().lastName(lastName)
                        .build());
        return dynamoDBMapper.query(User.class, queryExpression)
                .stream()
                .map(MAPPER::mapperUserToUser)
                .collect(Collectors.toList());
    }

    @Override
    public void updateUser(eu.luminis.breed.dynamodbmigration.user.model.User user) {
        try {
            dynamoDBMapper.save(MAPPER.userToMapperUser(user),
                    DynamoDBMapperConfig.builder()
                            .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(this.tableName))
                            .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES).build());
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to update user with id {}", user.getId(), e);
        }
    }

    //Not possible with mapper; see https://github.com/aws/aws-sdk-java/issues/534
    @Override
    public void updateUserAdvanced(final eu.luminis.breed.dynamodbmigration.user.model.User user) {
        final UpdateItemRequest updateItemRequest = UserMapper.updateItemRequest(user, tableName);
        updateItemRequest.withConditionExpression(LAST_MODIFIED_EXPRESSION);
        try {
            amazonDynamoDB.updateItem(updateItemRequest);
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
            dynamoDBMapper.delete(User.builder().id(id).build());
        } catch (Exception e) {
            throw UserException.error("Something went wrong when trying to delete user with id {}", id, e);
        }
    }
}
