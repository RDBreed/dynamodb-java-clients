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

import javax.annotation.PostConstruct;
import java.util.List;

@Configuration
@Profile("local")
public class AwsLocalConfiguration {

    private final String region;
    private final String key;
    private final String secret;

    public AwsLocalConfiguration(@Value("${cloud.aws.region.static}") String region,
                                 @Value("${cloud.aws.credentials.access-key}") String key,
                                 @Value("${cloud.aws.credentials.secret-key}") String secret) {
        this.region = region;
        this.key = key;
        this.secret = secret;
    }

    @Bean
    public List<UserRepository> userRepositories(@Value("${cloud.aws.dynamodb.tablename.user}") String tableName,
                                                 @Value("${cloud.aws.dynamodb.endpoint}") String endpoint,
                                                 @Value("${cloud.aws.dynamodb.port}") Integer port) {
        String endpointWithPort = endpoint + ":" + port;
        return List.of(new UserRepositoryDynamoDBSDK1LowLevelImpl(tableName, endpointWithPort),
                new UserRepositoryDynamoDBSDK1HighLevelImpl(tableName, endpointWithPort),
                new UserRepositoryDynamoDBSDK2LowLevelImpl(tableName, endpointWithPort),
                new UserRepositoryDynamoDBSDK2HighLevelImpl(tableName, endpointWithPort));
    }

    @Bean
    public List<UserAsyncRepository> userAsyncRepositories(@Value("${cloud.aws.dynamodb.tablename.user}") String tableName,
                                                           @Value("${cloud.aws.dynamodb.endpoint}") String endpoint,
                                                           @Value("${cloud.aws.dynamodb.port}") Integer port) {
        String endpointWithPort = endpoint + ":" + port;
        return List.of(new UserAsyncRepositoryDynamoDBSDK1LowLevelImpl(tableName, endpointWithPort),
                new UserAsyncRepositoryDynamoDBSDK1HighLevelImpl(tableName, endpointWithPort),
                new UserAsyncRepositoryDynamoDBSDK2LowLevelImpl(tableName, endpointWithPort),
                new UserAsyncRepositoryDynamoDBSDK2HighLevelImpl(tableName, endpointWithPort));
    }

    /**
     * Needed so that the Aws chain providers pickup the settings we put in the spring configuration...
     */
    @PostConstruct
    public void setProperties() {
        System.setProperty("aws.region", region);
        System.setProperty("aws.key", key);
        System.setProperty("aws.secret", secret);
    }
}
