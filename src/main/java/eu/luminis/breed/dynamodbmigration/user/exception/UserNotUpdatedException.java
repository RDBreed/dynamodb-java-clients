package eu.luminis.breed.dynamodbmigration.user.exception;

import java.util.UUID;

public class UserNotUpdatedException extends UserException {
    public static UserException error(UUID userId, Exception e) {
        return UserException.error("Something went wrong when trying to update user with id {}", userId, e);
    }
    public static UserException errorConditionCheck(UUID userId) {
        return UserException.error("User could not be updated as the update condition failed for user with id {}", userId);
    }
}
