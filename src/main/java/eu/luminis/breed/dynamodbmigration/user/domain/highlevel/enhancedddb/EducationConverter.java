package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static eu.luminis.breed.dynamodbmigration.user.util.ObjectMapperUtil.OBJECT_MAPPER;

@Slf4j
public class EducationConverter implements AttributeConverter<Education> {

    @Override
    public AttributeValue transformFrom(Education object) {
        if (object != null) {
            try {
                return AttributeValue.builder().s(OBJECT_MAPPER.writeValueAsString(object)).build();
            } catch (JsonProcessingException e) {
                log.error("Could not map object for teaser to attribute", e);
            }
        }
        return AttributeValue.builder().nul(true).build();
    }

    @Override
    public Education transformTo(AttributeValue input) {
        if (input != null && input.s() != null) {
            try {
                return OBJECT_MAPPER.readValue(input.s(), Education.class);
            } catch (JsonProcessingException e) {
                log.error("Could not map '{}' for teaser to object", input.s(), e);
            }
        }
        return null;
    }

    @Override
    public EnhancedType<Education> type() {
        return EnhancedType.of(Education.class);
    }

    @Override
    public AttributeValueType attributeValueType() {
        return AttributeValueType.S;
    }
}
