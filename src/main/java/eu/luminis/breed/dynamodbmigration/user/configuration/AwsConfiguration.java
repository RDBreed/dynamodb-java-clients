package eu.luminis.breed.dynamodbmigration.user.configuration;

import eu.luminis.breed.dynamodbmigration.user.repository.UserRepository;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK1HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK1LowLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK2HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK2LowLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.async.UserAsyncRepository;
import eu.luminis.breed.dynamodbmigration.user.repository.async.UserAsyncRepositoryDynamoDBSDK1HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.async.UserAsyncRepositoryDynamoDBSDK1LowLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.async.UserAsyncRepositoryDynamoDBSDK2HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.async.UserAsyncRepositoryDynamoDBSDK2LowLevelImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;

@Configuration
@Profile("!local")
public class AwsConfiguration {

    @Bean
    public List<UserRepository> userRepositories(@Value("${cloud.aws.dynamodb.tablename.user}") String tableName) {
        return List.of(new UserRepositoryDynamoDBSDK1LowLevelImpl(tableName),
                new UserRepositoryDynamoDBSDK1HighLevelImpl(tableName),
                new UserRepositoryDynamoDBSDK2LowLevelImpl(tableName),
                new UserRepositoryDynamoDBSDK2HighLevelImpl(tableName));
    }

    @Bean
    public List<UserAsyncRepository> userAsyncRepositories(@Value("${cloud.aws.dynamodb.tablename.user}") String tableName) {
        return List.of(new UserAsyncRepositoryDynamoDBSDK1LowLevelImpl(tableName),
                new UserAsyncRepositoryDynamoDBSDK1HighLevelImpl(tableName),
                new UserAsyncRepositoryDynamoDBSDK2LowLevelImpl(tableName),
                new UserAsyncRepositoryDynamoDBSDK2HighLevelImpl(tableName));
    }
}
