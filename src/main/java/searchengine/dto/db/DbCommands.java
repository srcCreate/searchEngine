package searchengine.dto.db;

import java.sql.*;

// Класс для взаимодействия с БД
public class DbCommands {

    public void updateDbData(String table, String column, String newData, String targetField, String targetValue) {
        String sqlUpdate = "UPDATE " + table + " SET " + column + "='" + newData + "' WHERE " +
                targetField +"='" + targetValue + "'";
        try (Connection connection = getNewConnection()){
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sqlUpdate);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteFromDb(String table, String column, String data) {
        String sqlSelect = "DELETE FROM " + table + " WHERE " + column + "='" + data + "'";
        try (Connection connection = getNewConnection()){
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getNewConnection() throws SQLException {
        String url = "jdbc:mysql://localhost/search_engine";
        String user = "root";
        String passwd = "pass";
        return DriverManager.getConnection(url, user, passwd);
    }
}
