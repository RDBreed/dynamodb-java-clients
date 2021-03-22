package eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v1;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.Address;
import eu.luminis.breed.dynamodbmigration.user.model.Education;
import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import eu.luminis.breed.dynamodbmigration.user.util.SafeConversionUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static eu.luminis.breed.dynamodbmigration.user.domain.UserFields.*;
import static eu.luminis.breed.dynamodbmigration.user.repository.UserRepository.putInItem;
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
        final Map<String, AttributeValueUpdate> attributeUpdates = attributeValueUpdates(user);
        updateItemRequest.withAttributeUpdates(attributeUpdates);
        return updateItemRequest;
    }

    public static Map<String, AttributeValue> mapToItem(User user, boolean includeKey) {
        Map<String, AttributeValue> item = new HashMap<>();
        if (includeKey) {
            item.put(ID_FIELD, new AttributeValue(SafeConversionUtil.safelyConvertToString(user.getId())));
        }
        putInItem(user.getFirstName(), getStringConsumerItem(item, FIRST_NAME_FIELD));
        putInItem(user.getLastName(), getStringConsumerItem(item, LAST_NAME_FIELD));
        putInItem(user.getAge(), getIntegerConsumerItem(item, AGE_FIELD));
        putInItem(user.getAddress(), getAddressConsumerItem(item, ADDRESS_FIELD));
        putInItem(user.getEducation(), getEducationConsumerItem(item, EDUCATION_FIELD));
        putInItem(user.getIsAdmin(), getBooleanConsumerItem(item, IS_ADMIN_FIELD));
        putInItem(SafeConversionUtil.safelyConvertToString(user.getGender()), getStringConsumerItem(item, GENDER_FIELD));
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

    private static Map<String, AttributeValueUpdate> attributeValueUpdates(User user) {
        return mapToItem(user, false)
                .entrySet()
                .stream()
                .map(entry -> entry(entry.getKey(), new AttributeValueUpdate().withValue(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Consumer<String> getStringConsumerItem(Map<String, AttributeValue> item, String firstNameField) {
        return value -> item.put(firstNameField, new AttributeValue(value));
    }

    private static Consumer<Integer> getIntegerConsumerItem(Map<String, AttributeValue> item, String fieldName) {
        return value -> item.put(fieldName, new AttributeValue().withN(SafeConversionUtil.safelyConvertToString(value)));
    }

    private static Consumer<Address> getAddressConsumerItem(Map<String, AttributeValue> item, String fieldName) {
        return value -> item.put(fieldName, new AttributeValue().withM(safelyConvertToMap(value)));
    }

    private static Map<String, AttributeValue> safelyConvertToMap(Address address) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        putInItem(address.getStreet(), getStringConsumerItem(attributeValueMap, ADDRESS_STREET_FIELD));
        putInItem(address.getCity(), getStringConsumerItem(attributeValueMap, ADDRESS_CITY_FIELD));
        putInItem(address.getNumber(), number -> attributeValueMap.put(ADDRESS_NUMBER_FIELD,
                new AttributeValue().withN(SafeConversionUtil.safelyConvertToString(number))));
        putInItem(address.getZipCode(), getStringConsumerItem(attributeValueMap, ADDRESS_ZIPCODE_FIELD));
        return attributeValueMap;
    }

    private static Consumer<Education> getEducationConsumerItem(Map<String, AttributeValue> item, String fieldName) {
        return value -> item.put(fieldName, new AttributeValue().withS(safelyConvertToString(value)));
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

    private static Consumer<Boolean> getBooleanConsumerItem(Map<String, AttributeValue> item, String fieldName) {
        return value -> item.put(fieldName, new AttributeValue().withBOOL(value));
    }

    private static Address mapToAddress(Map<String, AttributeValue> item) {
        final AttributeValue attributeValue = item.get(ADDRESS_FIELD);
        if (attributeValue != null && attributeValue.getM() != null) {
            final Map<String, AttributeValue> attributeValueM = attributeValue.getM();
            return Address.builder()
                    .street(safelyConvertToString(attributeValueM.get(ADDRESS_STREET_FIELD)))
                    .street(safelyConvertToString(attributeValueM.get(ADDRESS_CITY_FIELD)))
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
