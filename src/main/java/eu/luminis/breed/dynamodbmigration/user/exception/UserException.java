package eu.luminis.breed.dynamodbmigration.user.exception;

import java.text.MessageFormat;
import java.util.Arrays;

public class UserException extends RuntimeException {

    public static UserException error(String message, Object... args) {
        Object[] replacingObjects;
        if (lastReplacementIsThrowable(args)) {
            replacingObjects = Arrays.copyOf(args, getLastIndex(args));
            return new UserException(formatString(message, replacingObjects), (Throwable) getLastReplacement(args));
        } else {
            return new UserException(formatString(message, args));
        }
    }

    public static UserException errorIdIsNull(){
        return new UserException("Id of user may not be null");
    }

    private UserException(String message) {
        super(message);
    }

    public UserException(String message, Throwable cause) {
        super(message, cause);
    }

    private static String formatString(String originalMessage, Object... replacingObjects) {
        if(replacingObjects != null && replacingObjects.length > 0) {
            String stringToReturn = originalMessage;
            for (int i = 0; i < replacingObjects.length; i++) {
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
