package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGenerateStrategy;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBGeneratedUuid;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConvertedEnum;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@DynamoDBTable(tableName = "tableName")
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

    @DynamoDBHashKey
    @DynamoDBGeneratedUuid(value = DynamoDBAutoGenerateStrategy.CREATE)
    public UUID getId() {
        return id;
    }

    @DynamoDBIndexHashKey(globalSecondaryIndexName = "lastNameIndex")
    public String getLastName() {
        return lastName;
    }

    @DynamoDBTypeConverted(converter = EducationConverter.class)
    public Education getEducation(){
        return education;
    }

    /**
     * "The standard V1 and V2 compatible conversion schemas will by default
     * serialize booleans using the DynamoDB {@code N} type, with a value of '1'
     * representing 'true' and a value of '0' representing 'false'."
     *
     * @see com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped
     **/
    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.BOOL)
    @DynamoDBAttribute
    public Boolean getIsAdmin() {
        return isAdmin;
    }

    @DynamoDBTypeConvertedEnum
    public Gender getGender() {
        return gender;
    }
}
