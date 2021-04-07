package eu.luminis.breed.dynamodbmigration.user.configuration;

import eu.luminis.breed.dynamodbmigration.user.repository.UserRepository;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK1HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK1LowLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK2HighLevelImpl;
import eu.luminis.breed.dynamodbmigration.user.repository.UserRepositoryDynamoDBSDK2LowLevelImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class AwsConfiguration {
    @Bean
    public List<UserRepository> userRepositories(@Value("${aws.dynamodb.tablename.user}") String tableName,
                                                 @Value("${aws.endpoint}") String endpoint,
                                                 @Value("${aws.dynamodb.port}") Integer port){
        String endpointWithPort = endpoint + ":" + port;
        return List.of(new UserRepositoryDynamoDBSDK1LowLevelImpl(tableName, endpointWithPort),
                new UserRepositoryDynamoDBSDK1HighLevelImpl(tableName, endpointWithPort),
                new UserRepositoryDynamoDBSDK2LowLevelImpl(tableName, endpointWithPort),
                new UserRepositoryDynamoDBSDK2HighLevelImpl(tableName, endpointWithPort));
    }
}
