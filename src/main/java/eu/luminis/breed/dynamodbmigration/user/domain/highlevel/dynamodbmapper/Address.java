package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@DynamoDBDocument
@AllArgsConstructor
@NoArgsConstructor
public class Address {
    private String country;
    private String province;
    private String street;
    private Integer number;
    private String zipCode;
    private String city;
}
