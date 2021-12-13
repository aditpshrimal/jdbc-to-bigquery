package org.example.analytics;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;

/**
 * A sample app that connects to a Cloud SQL instance and lists all available tables
 in a database.
 */

public class CloudSqlConnector {
    public static String driverClassName;
    public static String jdbcUrl2;
    public static String username2;
    public static String password2;
    public static String sqlQuery;
    public static String bigqueryDataset;
    public static void main(String[] args) throws SQLNonTransientConnectionException
            , IOException, SQLException {

        String username = "root";
        String password = "password123";

        String jdbcUrl = "jdbc:mysql://google/classicmodels?cloudSqlInstance=future-sunrise-333208:asia-south1:tink-poc-sql&socketFactory=com.google.cloud.sql.mysql.SocketFactory";

        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM stage_params");
            while (resultSet.next()) {
                driverClassName = resultSet.getString(1);
                jdbcUrl2 = resultSet.getString(2);
                username2 = resultSet.getString(3);
                password2 = resultSet.getString(4);
                sqlQuery = resultSet.getString(5);
                bigqueryDataset = resultSet.getString(6);
                System.out.println(driverClassName+jdbcUrl2+username2+password2+sqlQuery+bigqueryDataset);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
