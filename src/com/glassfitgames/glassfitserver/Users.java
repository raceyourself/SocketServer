package com.glassfitgames.glassfitserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

public class Users {
    private final DataSource database;
    // NOTE: MySQL specific time handling
    private final String USER_ID_FROM_TOKEN = "SELECT resource_owner_id FROM oauth_access_tokens WHERE TIME_TO_SEC(TIMEDIFF(now(), created_at)) < expires_in AND revoked_at IS NULL AND token = ?";
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
        printWarnings(userIdFromToken.getWarnings());
        if (!result.next()) return null;
        int userId = result.getInt(1);
        printWarnings(result.getWarnings());
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
    
    private void printWarnings(SQLWarning warning) {
        while(warning != null) {
            System.err.println("\nSQL Warning:");
            System.err.println(warning.getMessage());
            System.err.println("ANSI-92 SQL State: " + warning.getSQLState());
            System.err.println("Vendor Error Code: " + warning.getErrorCode());
            warning = warning.getNextWarning();
        }
    }
}
