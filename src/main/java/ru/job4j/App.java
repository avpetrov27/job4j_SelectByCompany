package ru.job4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class App {
    private final Connection connection;

    public App(Properties config) {
        try {
            Class.forName(config.getProperty("driver-class-name"));
            connection = DriverManager.getConnection(
                    config.getProperty("url"),
                    config.getProperty("username"),
                    config.getProperty("password")
            );
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void personsAndCompanies() {
        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "select p.name personName, c.name companyName"
                                     + " from person p"
                                     + " left join company c on p.company_id = c.id"
                                     + " where (p.company_id <> 5 or p.company_id is null)")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                System.out.println("personName | companyName");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString("personName") + " | "
                            + resultSet.getString("companyName"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void companiesWithMaxEmployees() {
        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "select c.name, p.cnt quantity"
                                     + " from"
                                     + " (select company_id, "
                                     + " count(*) cnt ,"
                                     + " RANK () OVER ( ORDER BY count(*) DESC) rang "
                                     + " from person"
                                     + " group by company_id) p"
                                     + " left join company c on p.company_id = c.id"
                                     + " where p.rang = 1")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                System.out.println("companyName | quantity");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString("name") + " | "
                            + resultSet.getString("quantity"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        var config = new Properties();
        try (InputStream input = App.class.getClassLoader()
                .getResourceAsStream("prod.properties")) {
            config.load(input);
        }
        App app = new App(config);
        app.personsAndCompanies();
        System.out.println();
        app.companiesWithMaxEmployees();
    }
}

