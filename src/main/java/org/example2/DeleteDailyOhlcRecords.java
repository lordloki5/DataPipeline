package org.example2;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeleteDailyOhlcRecords {
    // Database connection URL
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";

    public static void main(String[] args) {
        System.out.println("Deleting all records from daily_ohlc:");

        // Use try-with-resources to manage the database connection
        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            // SQL query to delete all records
            String sql = "DELETE FROM daily_ohlc";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int deletedRows = pstmt.executeUpdate(); // Executes the delete query
                System.out.println("Total records deleted: " + deletedRows);
            }
        } catch (SQLException e) {
            // Handle any database errors
            System.err.println("Error deleting records from the database: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
