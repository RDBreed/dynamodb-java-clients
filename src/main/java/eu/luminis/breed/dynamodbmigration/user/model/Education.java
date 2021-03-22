package eu.luminis.breed.dynamodbmigration.user.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class Education {
    private Address primarySchool;
    private Address secondarySchool;
    private Address university;
}
