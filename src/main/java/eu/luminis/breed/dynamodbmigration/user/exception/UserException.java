package eu.luminis.breed.dynamodbmigration.user.exception;

import java.text.MessageFormat;
import java.util.Arrays;

public class UserException extends RuntimeException {

    private boolean isClientError = false;

    protected UserException() {
    }

    public static UserException error(String message, Object... args) {
        Object[] replacingObjects;
        if (lastReplacementIsThrowable(args)) {
            replacingObjects = Arrays.copyOf(args, getLastIndex(args));
            return new UserException(formatString(message, replacingObjects), (Throwable) getLastReplacement(args));
        } else {
            return new UserException(formatString(message, args));
        }
    }

    public static UserException clientError(String message, Object... args) {
        Object[] replacingObjects;
        if (lastReplacementIsThrowable(args)) {
            replacingObjects = Arrays.copyOf(args, getLastIndex(args));
            return new UserException(formatString(message, replacingObjects), (Throwable) getLastReplacement(args), true);
        } else {
            return new UserException(formatString(message, args), true);
        }
    }

    public static UserException errorIdIsNull(){
        return new UserException("Id of user may not be null");
    }

    private UserException(String message) {
        super(message);
        isClientError = false;
    }

    private UserException(String message, boolean isClientError) {
        super(message);
        this.isClientError = isClientError;
    }

    private UserException(String message, Throwable cause) {
        super(message, cause);
        isClientError = false;
    }

    private UserException(String formatString, Throwable throwable, boolean isClientError) {
        super(formatString, throwable);
        this.isClientError = isClientError;
    }

    private static String formatString(String originalMessage, Object... replacingObjects) {
        if(replacingObjects != null && replacingObjects.length > 0) {
            var stringToReturn = originalMessage;
            for (var i = 0; i < replacingObjects.length; i++) {
                stringToReturn = originalMessage.replaceFirst("\\{}", "{" + i + "}");
            }
            return MessageFormat.format(stringToReturn, replacingObjects);
        }
        return originalMessage;
    }

    private static boolean lastReplacementIsThrowable(Object[] replacements) {
        return replacements != null && replacements.length > 0 && getLastReplacement(replacements) instanceof Throwable;
    }

    private static Object getLastReplacement(Object[] replacements) {
        return replacements[getLastIndex(replacements)];
    }

    private static int getLastIndex(Object[] replacements) {
        return replacements.length - 1;
    }
}
