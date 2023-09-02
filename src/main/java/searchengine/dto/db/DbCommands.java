package searchengine.dto.db;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Класс для взаимодействия с БД
public class DbCommands {
    private final Connection connection;

    {
        try {
            connection = getNewConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet selectAllFromDb(String table) {
        String sqlSelect = "SELECT * FROM " + table;
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet selectFromDbWithParameters(String tableName, String columnName, String data) {
        String sqlSelect = "SELECT * FROM " + tableName + " WHERE " + columnName + "='" + data + "'";
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet selectFromDbWithTwoParameters(String tableName, String columnName1, String columnName2,
                                                   String data1, String data2) {
        String sqlSelect = "SELECT * FROM " + tableName + " WHERE " + columnName1 + "='" + data1 + "' and " + columnName2 + "='" + data2 + "'";
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet selectFromDbLessThan(String tableName, String columnName1, String columnName2, String data1, int data2) {
        String sqlSelect = "SELECT * FROM " + tableName + " WHERE `" + columnName1 + "` = '" + data1 + "' and `" + columnName2 + "` < '" + data2 + "'";
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet selectUrlLikeFromDB(Pattern pattern, String url, String table, String column) {
        Matcher matcher = pattern.matcher(url);
        String rootUrl = "";
        if (matcher.find()) {
            rootUrl = matcher.group();
        }
        String sqlSelect = "SELECT * FROM " + table + " WHERE " + column + " LIKE '" + rootUrl + "%' LIMIT 1";
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet selectCountWithParameter(String tableName, String columnName, String data) {
        String sqlSelect = "SELECT COUNT(*) FROM " + tableName + " WHERE " + columnName + "='" + data + "'";
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateDbData(String table, String column, String newData, String targetField, String targetValue) {
        String sqlUpdate = "UPDATE " + table + " SET " + column + "='" + newData + "' WHERE " +
                targetField +"='" + targetValue + "'";
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sqlUpdate);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteFromDb(String table, String column, String data) {
        String sqlSelect = "DELETE FROM " + table + " WHERE " + column + "='" + data + "'";
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getNewConnection() throws SQLException {
        String url = "jdbc:mysql://localhost/search_engine";
        String user = "root";
        String passwd = "pass";
        return DriverManager.getConnection(url, user, passwd);
    }


}
