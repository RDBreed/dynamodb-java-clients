package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Education {
    private Address primarySchool;
    private Address secondarySchool;
    private Address university;
}
