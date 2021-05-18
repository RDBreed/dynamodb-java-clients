package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.dynamodbmapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;

public class LocalDateTimeConverter implements DynamoDBTypeConverter<String, LocalDateTime> {

    @Override
    public String convert(LocalDateTime localDateTime) {
        if (localDateTime != null) {
            return localDateTime.toString();
        }
        return null;
    }

    @Override
    public LocalDateTime unconvert(String object) {
        if (!StringUtils.isEmpty(object)) {
            LocalDateTime.parse(object);
        }
        return null;
    }
}
