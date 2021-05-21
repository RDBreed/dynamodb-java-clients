package eu.luminis.breed.dynamodbmigration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class DynamodbMigrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamodbMigrationApplication.class, args);
    }

}
