package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;

import static eu.luminis.breed.dynamodbmigration.user.util.ObjectMapperUtil.OBJECT_MAPPER;

public class EducationConverter implements DynamoDBTypeConverter<String, Education> {

    @Override
    public String convert(Education education) {
        try {
            return OBJECT_MAPPER.writeValueAsString(education);
        } catch (JsonProcessingException e) {
            throw UserException.error("Cannot convert to string {}", education);
        }
    }

    @Override
    public Education unconvert(String object) {
        try {
            return OBJECT_MAPPER.readValue(object, Education.class);
        } catch (JsonProcessingException e) {
            throw UserException.error("Cannot convert to address {}", object);
        }
    }
}
