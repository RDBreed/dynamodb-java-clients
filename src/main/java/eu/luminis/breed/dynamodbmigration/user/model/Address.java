package eu.luminis.breed.dynamodbmigration.user.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Address {
    private String country;
    private String province;
    private String city;
    private String street;
    private Integer number;
    private String zipCode;
}
