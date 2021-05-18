package eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v1;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.Address;
import eu.luminis.breed.dynamodbmigration.user.model.Education;
import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import eu.luminis.breed.dynamodbmigration.user.util.SafeConversionUtil;
import eu.luminis.breed.dynamodbmigration.user.util.TimeMachine;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.*;
import static eu.luminis.breed.dynamodbmigration.user.util.ObjectMapperUtil.OBJECT_MAPPER;
import static java.util.Map.entry;

public class UserMapper {
    private UserMapper() {
    }

    public static UpdateItemRequest updateItemRequest(User user, String tableName) {
        if (user.getId() == null) {
            throw UserException.errorIdIsNull();
        }
        final UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .addKeyEntry(ID_FIELD, new AttributeValue(SafeConversionUtil.safelyConvertToString(user.getId())));
        final Map<String, AttributeValue> attributeUpdates = attributeValueUpdates(user);
        updateItemRequest.setExpressionAttributeValues(attributeUpdates);
        updateItemRequest.setUpdateExpression(getUpdateExpression(user));
        //The following is easier, but deprecated (and not possible to combine with conditional expressions):
        //updateItemRequest.withAttributeUpdates(attributeUpdates);
        return updateItemRequest;
    }

    public static Map<String, AttributeValue> mapToItem(User user, boolean includeKey) {
        Map<String, AttributeValue> item = new HashMap<>();
        if (includeKey) {
            item.put(ID_FIELD, new AttributeValue(SafeConversionUtil.safelyConvertToString(user.getId())));
        }
        Optional.ofNullable(user.getFirstName()).ifPresent(value -> item.put(FIRST_NAME_FIELD, new AttributeValue(value)));
        Optional.ofNullable(user.getLastName()).ifPresent(value -> item.put(LAST_NAME_FIELD, new AttributeValue(value)));
        Optional.ofNullable(user.getAge()).ifPresent(value -> item.put(AGE_FIELD, new AttributeValue().withN(String.valueOf(value))));
        Optional.ofNullable(user.getAddress()).ifPresent(value -> item.put(ADDRESS_FIELD, new AttributeValue().withM(safelyConvertToMap(value))));
        Optional.ofNullable(user.getEducation()).ifPresent(value -> item.put(EDUCATION_FIELD, new AttributeValue(safelyConvertToString(value))));
        Optional.ofNullable(user.getIsAdmin()).ifPresent(value -> item.put(IS_ADMIN_FIELD, new AttributeValue().withBOOL(value)));
        Optional.ofNullable(user.getGender()).ifPresent(value -> item.put(GENDER_FIELD, new AttributeValue(String.valueOf(value))));
        //as this method is always used for saving/updating, always change last modified to now
        item.put(LAST_MODIFIED_FIELD, new AttributeValue(String.valueOf(String.valueOf(TimeMachine.now()))));
        return item;
    }

    public static User mapToUser(Map<String, AttributeValue> item) {
        final Address address = mapToAddress(item);
        final Education education = mapToEducation(item.get(EDUCATION_FIELD));
        return User.builder()
                .id(safelyConvertToUUID(item.get(ID_FIELD)))
                .firstName(safelyConvertToString(item.get(FIRST_NAME_FIELD)))
                .lastName(safelyConvertToString(item.get(LAST_NAME_FIELD)))
                .age(safelyConvertToInteger(item.get(AGE_FIELD)))
                .address(address)
                .education(education)
                .isAdmin(safelyConvertToBoolean(item.get(IS_ADMIN_FIELD)))
                .gender(safelyConvertToGender(item.get(GENDER_FIELD)))
                .build();

    }

    private static Map<String, AttributeValue> attributeValueUpdates(User user) {
        return mapToItem(user, false)
                .entrySet()
                .stream()
                .map(entry -> entry(":" + entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static String getUpdateExpression(User user) {
        final Map<String, AttributeValue> attributeUpdates = mapToItem(user, false);
        final StringBuilder updateExpression = new StringBuilder();
        updateExpression.append("SET ");
        for (Map.Entry<String, AttributeValue> stringAttributeValueUpdateEntry : attributeUpdates.entrySet()) {
            updateExpression.append(stringAttributeValueUpdateEntry.getKey()).append("=:").append(stringAttributeValueUpdateEntry.getKey()).append(", ");
        }
        updateExpression.deleteCharAt(updateExpression.lastIndexOf(","));
        return updateExpression.toString();
    }

    private static Map<String, AttributeValue> safelyConvertToMap(Address address) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        Optional.ofNullable(address.getCountry()).ifPresent(value -> attributeValueMap.put(ADDRESS_COUNTRY_FIELD, new AttributeValue(value)));
        Optional.ofNullable(address.getProvince()).ifPresent(value -> attributeValueMap.put(ADDRESS_PROVINCE_FIELD, new AttributeValue(value)));
        Optional.ofNullable(address.getStreet()).ifPresent(value -> attributeValueMap.put(ADDRESS_STREET_FIELD, new AttributeValue(value)));
        Optional.ofNullable(address.getCity()).ifPresent(value -> attributeValueMap.put(ADDRESS_CITY_FIELD, new AttributeValue(value)));
        Optional.ofNullable(address.getNumber()).ifPresent(value -> attributeValueMap.put(ADDRESS_NUMBER_FIELD, new AttributeValue().withN(String.valueOf(value))));
        Optional.ofNullable(address.getZipCode()).ifPresent(value -> attributeValueMap.put(ADDRESS_ZIPCODE_FIELD, new AttributeValue(value)));
        return attributeValueMap;
    }

    private static String safelyConvertToString(Education education) {
        if (education != null) {
            try {
                return OBJECT_MAPPER.writeValueAsString(education);
            } catch (JsonProcessingException e) {
                throw UserException.error("Could not convert {} to String.", EDUCATION_FIELD, e);
            }
        }
        return null;
    }

    private static Address mapToAddress(Map<String, AttributeValue> item) {
        final AttributeValue attributeValue = item.get(ADDRESS_FIELD);
        if (attributeValue != null && attributeValue.getM() != null) {
            final Map<String, AttributeValue> attributeValueM = attributeValue.getM();
            return Address.builder()
                    .country(safelyConvertToString(attributeValueM.get(ADDRESS_COUNTRY_FIELD)))
                    .province(safelyConvertToString(attributeValueM.get(ADDRESS_PROVINCE_FIELD)))
                    .street(safelyConvertToString(attributeValueM.get(ADDRESS_STREET_FIELD)))
                    .city(safelyConvertToString(attributeValueM.get(ADDRESS_CITY_FIELD)))
                    .number(safelyConvertToInteger(attributeValueM.get(ADDRESS_NUMBER_FIELD)))
                    .zipCode(safelyConvertToString(attributeValueM.get(ADDRESS_ZIPCODE_FIELD)))
                    .build();
        }
        return null;
    }

    private static Education mapToEducation(AttributeValue attributeValue) {
        if (attributeValue != null && attributeValue.getS() != null) {
            try {
                OBJECT_MAPPER.readValue(attributeValue.getS(), Education.class);
            } catch (JsonProcessingException e) {
                throw UserException.error("Could not read property {}", EDUCATION_FIELD, e);
            }
        }
        return null;
    }

    private static Boolean safelyConvertToBoolean(AttributeValue value) {
        return isNull(value) ? null : value.getBOOL();
    }

    private static String safelyConvertToString(AttributeValue value) {
        return isNull(value) ? null : value.getS();
    }

    private static UUID safelyConvertToUUID(AttributeValue value) {
        return isNull(value) ? null : UUID.fromString(value.getS());
    }

    private static Integer safelyConvertToInteger(AttributeValue value) {
        return isNull(value) ? null : Integer.valueOf(value.getN());
    }

    private static Gender safelyConvertToGender(AttributeValue value) {
        return isNull(value) ? null : Gender.valueOf(value.getS());
    }

    private static boolean isNull(AttributeValue attributeValue) {
        return attributeValue == null || (attributeValue.isNULL() != null && attributeValue.isNULL());
    }
}
