package com.PostgreSetup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ListIndices {
    // Database connection URL from your previous code
    static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/ohcldata";
    static final String USER = "dhruvbhandari";
    static final String PASS = "";
    public static void main(String[] args) {
        System.out.println("Listing all records from indices table:");

        // Use try-with-resources to manage the database connection
        try (Connection conn = DriverManager.getConnection(DEFAULT_DB_URL, USER, PASS)) {
            // SQL query to select all records from the indices table
            String sql = "SELECT * FROM indices";
            try (PreparedStatement pstmt = conn.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {
                int count = 0;
                // Print header
                System.out.println("------------------------------------------------");
                System.out.printf("%-12s %-20s %-20s%n",
                        "Index ID", "Index Name", "Exchange");
                System.out.println("------------------------------------------------");

                // Iterate through the result set and print each record
                while (rs.next()) {
                    // Retrieve column values based on the provided structure
                    int indexId = rs.getInt("index_id");
                    String indexName = rs.getString("index_name");
                    String exchange = rs.getString("exchange");

                    // Print the record details
                    System.out.printf("%-12d %-20s %-20s%n",
                            indexId, indexName, exchange);
                    count++;
                }
                // Print the total number of records
                System.out.println("------------------------------------------------");
                System.out.println("Total records: " + count);
            }
        } catch (SQLException e) {
            // Handle any database errors
            System.err.println("Error accessing the database: " + e.getMessage());
            e.printStackTrace();
        }
    }
}