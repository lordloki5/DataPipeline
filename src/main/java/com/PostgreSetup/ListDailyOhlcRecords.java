package com.PostgreSetup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ListDailyOhlcRecords {
    // Database connection URL
    static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/ohcldata";
    static final String USER = "dhruvbhandari";
    static final String PASS = "";
    public static void main(String[] args) {
        System.out.println("Listing all records from daily_ohlc:");

        // Use try-with-resources to manage the database connection
        try (Connection conn = DriverManager.getConnection(DEFAULT_DB_URL, USER, PASS)) {
            // SQL query to select all records from the daily_ohlc table
            String sql = "SELECT * FROM daily_ohlc";
            try (PreparedStatement pstmt = conn.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {
                int count = 0;
                // Iterate through the result set and print each record
                while (rs.next()) {
                    // Retrieve column values (adjust names/types as per your table schema)
                    java.sql.Timestamp tradeDate = rs.getTimestamp("trade_date");
                    int indexId = rs.getInt("index_id");
                    double open = rs.getDouble("open_price");
                    double high = rs.getDouble("high_price");
                    double low = rs.getDouble("low_price");
                    double close = rs.getDouble("close_price");

                    // Print the record details
                    System.out.println("Trade Date: " + tradeDate +
                            ", Index ID: " + indexId +
                            ", Open: " + open +
                            ", High: " + high +
                            ", Low: " + low +
                            ", Close: " + close);
                    count++;
                }
                // Print the total number of records
                System.out.println("Total records: " + count);
            }
        } catch (SQLException e) {
            // Handle any database errors
            System.err.println("Error accessing the database: " + e.getMessage());
            e.printStackTrace();
        }
    }
}