package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@Data
@DynamoDbBean
@AllArgsConstructor
@NoArgsConstructor
public class Address {
    private String street;
    private Integer number;
    private String zipCode;
    private String city;
}
