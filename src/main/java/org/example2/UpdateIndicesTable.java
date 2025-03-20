package org.example2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UpdateIndicesTable {
    // Database connection URL from your previous code
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";

    public static void main(String[] args) {
        System.out.println("Updating indices table...");

        // Use try-with-resources to manage the database connection
        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            // Disable auto-commit to manage transaction manually
            conn.setAutoCommit(false);

            try {
                // Step 1: Delete referencing rows from daily_ohlc
                String deleteDailyOhlcSql = "DELETE FROM daily_ohlc WHERE index_id IN (" +
                        "SELECT index_id FROM indices ORDER BY index_id DESC LIMIT 2)";
                try (PreparedStatement deleteDailyOhlcStmt = conn.prepareStatement(deleteDailyOhlcSql)) {
                    int rowsDeletedDailyOhlc = deleteDailyOhlcStmt.executeUpdate();
                    System.out.println("Deleted " + rowsDeletedDailyOhlc + " rows from daily_ohlc");
                }

                // Step 2: Delete the last two entries from indices
                String deleteIndicesSql = "DELETE FROM indices WHERE index_id IN (" +
                        "SELECT index_id FROM indices ORDER BY index_id DESC LIMIT 2)";
                try (PreparedStatement deleteIndicesStmt = conn.prepareStatement(deleteIndicesSql)) {
                    int rowsDeletedIndices = deleteIndicesStmt.executeUpdate();
                    System.out.println("Deleted " + rowsDeletedIndices + " rows from indices");
                }

                // Step 3: Insert new entries for NIFTY 50 and BSE 500
                String insertSql = "INSERT INTO indices (index_name, exchange) VALUES (?, ?)";
//                try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
//                    // Insert NIFTY 50
//                    insertStmt.setString(1, "NIFTY 50");
//                    insertStmt.setString(2, "NSE");
//                    insertStmt.addBatch();
//
//                    // Insert BSE 500
//                    insertStmt.setString(1, "BSE 500");
//                    insertStmt.setString(2, "BSE");
//                    insertStmt.addBatch();
//
//                    // Execute batch insert
//                    int[] rowsInserted = insertStmt.executeBatch();
//                    System.out.println("Inserted " + rowsInserted.length + " new rows into indices");
//                }

                // Commit the transaction
                conn.commit();
                System.out.println("Transaction committed successfully");

            } catch (SQLException e) {
                // Roll back the transaction on error
                conn.rollback();
                System.err.println("Error during transaction, rolled back: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // Restore auto-commit mode
                conn.setAutoCommit(true);
            }

        } catch (SQLException e) {
            System.err.println("Error connecting to the database: " + e.getMessage());
            e.printStackTrace();
        }
    }
}