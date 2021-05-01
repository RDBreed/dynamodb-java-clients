package eu.luminis.breed.dynamodbmigration;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class TestUtil {
    public static DockerComposeContainer<?> localStackContainer;
    protected static final String tableName = "user";
    protected static String endpoint;
    protected static String region = "eu-west-1";
    protected static final String key = "key";
    protected static final String secret = "secret";
    protected static AmazonDynamoDB amazonDynamoDBClient;


    static {
        try {
            final Properties properties = getAllProperties();
            final Boolean isTestContainersEnabled = Boolean.valueOf(String.valueOf(properties.getOrDefault("testcontainers.enabled", "true")));
            if (isTestContainersEnabled) {
                createTestContainersSetup();
            } else {
                region = properties.getProperty("aws.region");
                endpoint = properties.getProperty("aws.endpoint") + ":" + properties.getProperty("dynamodb.port");
                amazonDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret)))
                        .build();
            }
            System.setProperty("aws.region", region);
            System.setProperty("aws.key", key);
            System.setProperty("aws.secret", secret);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTestContainersSetup() {
        final String serviceName = "localstack_1";
        final int servicePort = 4566;
        DockerComposeContainer<?> localStackContainer =
                new DockerComposeContainer<>(new File("./docker-compose.yml"))
                        .withExposedService(serviceName, servicePort)
                        .withLocalCompose(true);
        localStackContainer.start();
        endpoint = getEndpoint(serviceName, servicePort, localStackContainer);
        amazonDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret)))
                .build();
    }

    private static String getEndpoint(String serviceName, int servicePort, DockerComposeContainer<?> localStackContainer) {
        return "http://" +
                localStackContainer.getServiceHost(serviceName, servicePort) +
                ":" +
                localStackContainer.getServicePort(serviceName, servicePort);
    }

    /**
     * Needed because creation of the cdk stack can take some time. Preferably, would like to use a Docker health check, but
     * TestContainers does not seem to like that at the moment combined with docker-compose...
     */
    @BeforeAll
    static void waitForDynamoDBTable() {
        log.info("Waiting on table creation");
        final Waiter<DescribeTableRequest> waiter = amazonDynamoDBClient.waiters().tableExists();
        try {
            waiter.run(new WaiterParameters<>(new DescribeTableRequest(tableName)).withPollingStrategy(new PollingStrategy(new MaxAttemptsRetryStrategy(25), new FixedDelayStrategy(5))));
        } catch (Exception exception) {
            throw new IllegalArgumentException("Table " + tableName + " did not transition into ACTIVE state.", exception);
        }
        log.info("Table creation is done");
    }

    public static Properties getAllProperties() throws IOException {
        final Properties properties = new Properties();
        properties.load(TestUtil.class.getResourceAsStream("/localstack.properties"));
        Properties merged = new Properties();
        merged.putAll(properties);
        merged.putAll(System.getProperties());
        return merged;
    }
}
