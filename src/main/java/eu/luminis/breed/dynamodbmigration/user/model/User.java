package eu.luminis.breed.dynamodbmigration.user.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

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
}
