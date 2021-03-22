package eu.luminis.breed.dynamodbmigration.user.domain.highlevel.enhancedddb;

import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

@Mapper
public interface UserMapper {

    UserMapper MAPPER = Mappers.getMapper(UserMapper.class);

    User userToEnhancedUser(eu.luminis.breed.dynamodbmigration.user.model.User user);

    eu.luminis.breed.dynamodbmigration.user.model.User enhancedUserToUser(User user);
}
