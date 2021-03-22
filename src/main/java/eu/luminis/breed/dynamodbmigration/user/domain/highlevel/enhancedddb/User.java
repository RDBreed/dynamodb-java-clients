package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb;

import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;

import java.util.UUID;

@DynamoDbBean
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private UUID id;
    private String firstName;
    private String lastName;
    private Integer age;
    private Address address;
    private Education education;
    private Boolean isAdmin;
    private Gender gender;

    @DynamoDbPartitionKey
    public UUID getId() {
        return id;
    }

    @DynamoDbConvertedBy(EducationConverter.class)
    public Education getEducation() {
        return education;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = "lastNameIndex")
    public String getLastName(){
        return lastName;
    }
}
