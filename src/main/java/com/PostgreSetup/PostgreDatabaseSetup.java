package com.PostgreSetup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class PostgreDatabaseSetup {
    // Connection details for default 'postgres' DB
    static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    static final String TARGET_DB = "ohcldata"; // Database name to create/use
    static final String USER = "dhruvbhandari";
    static final String PASS = "";

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");

            // STEP 1: Connect to default 'postgres' database
            Connection defaultConn = DriverManager.getConnection(DEFAULT_DB_URL, USER, PASS);
            System.out.println("Connected to default 'postgres' database...");

            // STEP 2: Create 'ohcldata' database if not exists
            Statement stmt = defaultConn.createStatement();

            // Check if database exists
            String checkDbQuery = "SELECT 1 FROM pg_database WHERE datname = '" + TARGET_DB + "'";
            ResultSet rs = stmt.executeQuery(checkDbQuery);
            if (!rs.next()) {
                // Database doesn't exist, create it
                String createDb = "CREATE DATABASE " + TARGET_DB;
                stmt.executeUpdate(createDb);
                System.out.println("Database '" + TARGET_DB + "' created successfully...");
            } else {
                System.out.println("Database '" + TARGET_DB + "' already exists.");
            }

            rs.close();
            stmt.close();
            defaultConn.close();

            // STEP 3: Now connect to 'ohcldata' DB
            String targetDbUrl = "jdbc:postgresql://localhost:5432/" + TARGET_DB;
            Connection conn = DriverManager.getConnection(targetDbUrl, USER, PASS);
            System.out.println("Connected to '" + TARGET_DB + "' database...");

            Statement targetStmt = conn.createStatement();

            // 4. Create 'indices' table
            String createIndices = """
                    CREATE TABLE IF NOT EXISTS indices (
                        index_id SERIAL PRIMARY KEY,
                        index_name VARCHAR(50) UNIQUE NOT NULL,
                        exchange VARCHAR(10) NOT NULL
                    );
                    """;
            targetStmt.executeUpdate(createIndices);
            System.out.println("'indices' table created!");

            // 5. Create 'daily_ohlc' table
            String createDailyOhlc = """
                    CREATE TABLE IF NOT EXISTS daily_ohlc (
                        trade_date TIMESTAMPTZ NOT NULL,
                        index_id INT NOT NULL,
                        open_price DECIMAL(15, 2) NOT NULL,
                        high_price DECIMAL(15, 2) NOT NULL,
                        low_price DECIMAL(15, 2) NOT NULL,
                        close_price DECIMAL(15, 2) NOT NULL,
                        FOREIGN KEY (index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_index_date UNIQUE (index_id, trade_date)
                    );
                    """;
            targetStmt.executeUpdate(createDailyOhlc);
            System.out.println("'daily_ohlc' table created!");

            // Cleanup
            targetStmt.close();
            conn.close();

            System.out.println("\nAll Done!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
