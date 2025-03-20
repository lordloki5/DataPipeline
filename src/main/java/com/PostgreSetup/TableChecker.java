package com.PostgreSetup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TableChecker {
    // Database connection details
    static final String DB_URL = "jdbc:postgresql://localhost:5432/ohcldata";
    static final String USER = "dhruvbhandari";
    static final String PASS = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;

        try {
            // Register driver
            Class.forName("org.postgresql.Driver");

            // Connect to ohcldata
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected to database 'ohcldata' successfully!");

            // Create statement
            stmt = conn.createStatement();

            // Query to check tables
            String checkTablesQuery = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE';
            """;

            ResultSet rs = stmt.executeQuery(checkTablesQuery);

            System.out.println("\nExisting tables in 'ohcldata' database:");

            boolean foundTables = false;
            while (rs.next()) {
                foundTables = true;
                System.out.println("- " + rs.getString("table_name"));
            }

            if (!foundTables) {
                System.out.println("No tables found in 'public' schema.");
            }

            // Clean up
            rs.close();
            stmt.close();
            conn.close();

            System.out.println("\nTable check completed!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
