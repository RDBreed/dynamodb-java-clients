package eu.luminis.breed.dynamodbmigration.user.domain.lowlevel.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.luminis.breed.dynamodbmigration.user.exception.UserException;
import eu.luminis.breed.dynamodbmigration.user.model.Address;
import eu.luminis.breed.dynamodbmigration.user.model.Education;
import eu.luminis.breed.dynamodbmigration.user.model.Gender;
import eu.luminis.breed.dynamodbmigration.user.model.User;
import eu.luminis.breed.dynamodbmigration.user.util.SafeConversionUtil;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

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

    public static Map<String, AttributeValue> mapToItem(User user, boolean includeKey) {
        Map<String, AttributeValue> item = new HashMap<>();
        if (includeKey) {
            item.put(ID_FIELD, AttributeValue.builder().s(SafeConversionUtil.safelyConvertToString(user.getId())).build());
        }
        Optional.ofNullable(user.getFirstName()).ifPresent(value -> item.put(FIRST_NAME_FIELD, AttributeValue.builder().s(value).build()));
        Optional.ofNullable(user.getLastName()).ifPresent(value -> item.put(LAST_NAME_FIELD, AttributeValue.builder().s(value).build()));
        Optional.ofNullable(user.getAge()).ifPresent(value -> item.put(AGE_FIELD, AttributeValue.builder().n(String.valueOf(value)).build()));
        Optional.ofNullable(user.getAddress()).ifPresent(value -> item.put(ADDRESS_FIELD, AttributeValue.builder().m(safelyConvertToMap(value)).build()));
        Optional.ofNullable(user.getEducation()).ifPresent(value -> item.put(EDUCATION_FIELD, AttributeValue.builder().s(safelyConvertToString(value)).build()));
        Optional.ofNullable(user.getIsAdmin()).ifPresent(value -> item.put(IS_ADMIN_FIELD, AttributeValue.builder().bool(value).build()));
        Optional.ofNullable(user.getGender()).ifPresent(value -> item.put(GENDER_FIELD, AttributeValue.builder().s(String.valueOf(value)).build()));
        return item;
    }

    public static Map<String, AttributeValueUpdate> attributeValueUpdates(User user) {
        return mapToItem(user, false)
                .entrySet()
                .stream()
                .map(entry -> entry(entry.getKey(), AttributeValueUpdate.builder().value(entry.getValue()).build()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static User mapToUser(GetItemResponse response) {
        if (!response.hasItem()) {
            return null;
        }
        final Map<String, AttributeValue> item = response.item();
        return mapToUser(item);
    }

    public static User mapToUser(Map<String, AttributeValue> item) {
        return User.builder()
                .id(safelyConvertToUUID(item.get(ID_FIELD)))
                .firstName(safelyConvertToString(item.get(FIRST_NAME_FIELD)))
                .lastName(safelyConvertToString(item.get(LAST_NAME_FIELD)))
                .age(safelyConvertToInteger(item.get(AGE_FIELD)))
                .address(mapToAddress(item))
                .education(mapToEducation(item.get(EDUCATION_FIELD)))
                .isAdmin(safelyConvertToBoolean(item.get(IS_ADMIN_FIELD)))
                .gender(safelyConvertToGender(item.get(GENDER_FIELD)))
                .build();
    }

    private static Map<String, AttributeValue> safelyConvertToMap(Address address) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        Optional.ofNullable(address.getCountry()).ifPresent(value -> attributeValueMap.put(ADDRESS_COUNTRY_FIELD, AttributeValue.builder().s(value).build()));
        Optional.ofNullable(address.getProvince()).ifPresent(value -> attributeValueMap.put(ADDRESS_PROVINCE_FIELD, AttributeValue.builder().s(value).build()));
        Optional.ofNullable(address.getStreet()).ifPresent(value -> attributeValueMap.put(ADDRESS_STREET_FIELD, AttributeValue.builder().s(value).build()));
        Optional.ofNullable(address.getCity()).ifPresent(value -> attributeValueMap.put(ADDRESS_CITY_FIELD, AttributeValue.builder().s(value).build()));
        Optional.ofNullable(address.getNumber()).ifPresent(value -> attributeValueMap.put(ADDRESS_NUMBER_FIELD, AttributeValue.builder().n(String.valueOf(value)).build()));
        Optional.ofNullable(address.getZipCode()).ifPresent(value -> attributeValueMap.put(ADDRESS_ZIPCODE_FIELD, AttributeValue.builder().s(value).build()));
        return attributeValueMap;
    }

    private static Address mapToAddress(Map<String, AttributeValue> item) {
        final AttributeValue attributeValue = item.get(ADDRESS_FIELD);
        if (attributeValue != null && attributeValue.m() != null) {
            final Map<String, AttributeValue> attributeValueM = attributeValue.m();
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
        if (attributeValue != null && attributeValue.s() != null) {
            try {
                OBJECT_MAPPER.readValue(attributeValue.s(), Education.class);
            } catch (JsonProcessingException e) {
                throw UserException.error("Could not read property {}", EDUCATION_FIELD, e);
            }
        }
        return null;
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

    private static Boolean safelyConvertToBoolean(AttributeValue value) {
        return isNull(value) ? null : value.bool();
    }

    private static String safelyConvertToString(AttributeValue value) {
        return isNull(value) ? null : value.s();
    }

    private static UUID safelyConvertToUUID(AttributeValue value) {
        return isNull(value) ? null : UUID.fromString(value.s());
    }

    private static Integer safelyConvertToInteger(AttributeValue value) {
        return isNull(value) ? null : Integer.valueOf(value.n());
    }

    private static Gender safelyConvertToGender(AttributeValue value) {
        return isNull(value) ? null : Gender.valueOf(value.s());
    }

    private static boolean isNull(AttributeValue attributeValue) {
        return attributeValue == null || (attributeValue.nul() != null && attributeValue.nul());
    }
}
