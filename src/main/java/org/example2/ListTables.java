package org.example2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ListTables {
    // Connection URL from the provided HistoricalDataBse class
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";

    public static void main(String[] args) {
        System.out.println("Listing tables in the database:");

        // Use try-with-resources to automatically close the connection
        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            // Get database metadata
            DatabaseMetaData metaData = conn.getMetaData();

            // Retrieve all tables in the 'public' schema
            try (ResultSet rs = metaData.getTables(null, "public", "%", new String[]{"TABLE"})) {
                int count = 0;
                while (rs.next()) {
                    // Get the table name from the result set
                    String tableName = rs.getString("TABLE_NAME");
                    System.out.println(tableName);
                    count++;
                }
                System.out.println("Total tables: " + count);
            }
        } catch (SQLException e) {
            System.err.println("Error accessing the database: " + e.getMessage());
            e.printStackTrace();
        }
    }
}