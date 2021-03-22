package eu.luminis.breed.dynamodbmigration.user.util;

import eu.luminis.breed.dynamodbmigration.user.model.Gender;

import java.util.UUID;

public final class SafeConversionUtil {

    private SafeConversionUtil() {
    }

    public static String safelyConvertToString(UUID value) {
        return value != null ? value.toString() : null;
    }

    public static String safelyConvertToString(Integer value) {
        return value != null ? String.valueOf(value) : null;
    }

    public static String safelyConvertToString(Gender value) {
        return value != null ? String.valueOf(value) : null;
    }
}
