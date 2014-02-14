package com.glassfitgames.glassfitserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

public class Users {
    private final DataSource database;
    private final String USER_ID_FROM_TOKEN = "SELECT resource_owner_id FROM oauth_access_tokens WHERE expires_in < NOW() AND revoked_at IS NULL AND token = ?";
    private final ConcurrentHashMap<Integer, User> users = new ConcurrentHashMap<Integer, User>();
    
    public Users(DataSource database) throws SQLException {
        this.database = database;
    }
    
    public User fromToken(String token) {
        try {
            return fromTokenImpl(token);
        } catch (SQLException e) {
            e.printStackTrace();
            // TODO: Retry?
            throw new RuntimeException("Internal server error");
        }
    }
    
    private User fromTokenImpl(String token) throws SQLException {
        Connection c = database.getConnection();
        PreparedStatement userIdFromToken = c.prepareStatement(USER_ID_FROM_TOKEN);
        userIdFromToken.setString(1, token);
        ResultSet result = userIdFromToken.executeQuery();
        if (!result.next()) return null;
        int userId = result.getInt(1);
        result.close();
        c.close();
        User user = get(userId);
        if (user == null) user = createUser(userId);
        return user;
    }
    
    public User createUser(int id) {
        User user = new User(id);
        User existing = users.putIfAbsent(id, user);
        if (existing != null) return existing;
        else return user;
    }
    
    public User get(int id) {
        return users.get(id);
    }
}
