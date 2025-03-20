package com.PostgreSetup;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertSectoralIndices {
    public static void main(String[] args) {
        // Connection details for the 'ohlcData' database
        String url = "jdbc:postgresql://localhost:5432/ohcldata";
        String user = System.getProperty("user.name"); // Your macOS username
        String password = ""; // Empty by default for Homebrew PostgreSQL

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            if (conn != null) {
                System.out.println("Connected to 'ohlcData' database successfully!");

                // SQL statement for inserting into 'indices' table
                String insertSQL = "INSERT INTO indices (index_name, exchange) VALUES (?, ?) ON CONFLICT (index_name) DO NOTHING;";

                // Prepare the statement
                PreparedStatement pstmt = conn.prepareStatement(insertSQL);

                // Step 1: Insert NIFTY 50
                pstmt.setString(1, "NIFTY 50");
                pstmt.setString(2, "NSE");
                pstmt.addBatch();
                System.out.println("Added NIFTY 50 to batch.");

                // Step 2: Insert BSE 500
                pstmt.setString(1, "BSE 500");
                pstmt.setString(2, "BSE");
                pstmt.addBatch();
                System.out.println("Added BSE 500 to batch.");

                // Step 3: Insert NSE sectoral indices
                String[] nseSectoralIndices = {
                        "NIFTY AUTO", "NIFTY BANK", "NIFTY ENERGY", "NIFTY FINANCIAL SERVICES",
                        "NIFTY FMCG", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL", "NIFTY PHARMA",
                        "NIFTY PSU BANK", "NIFTY REALTY", "NIFTY PRIVATE BANK", "NIFTY HEALTHCARE INDEX",
                        "NIFTY CONSUMER DURABLES", "NIFTY OIL & GAS"
                };

                for (String indexName : nseSectoralIndices) {
                    pstmt.setString(1, indexName);
                    pstmt.setString(2, "NSE");
                    pstmt.addBatch();
                }
                System.out.println("Added " + nseSectoralIndices.length + " NSE sectoral indices to batch.");

                // Step 4: Insert BSE sectoral indices
                String[] bseSectoralIndices = {
                        "BSE AUTO",
                        "BSE BANKEX",
                        "BSE CAPITAL GOODS",
                        "BSE CARBONEX",
                        "BSE Commodities",
                        "BSE CONSUMER DISCRETIONARY",
                        "BSE CONSUMER DURABLES",
                        "BSE CPSE",
                        "BSE ENERGY",
                        "BSE FAST MOVING CONSUMER GOODS",
                        "BSE FINANCIAL SERVICES",
                        "BSE HEALTHCARE",
                        "BSE HOUSING INDEX",
                        "BSE INDIA INFRASTRUCTURE INDEX",
                        "BSE INDIA MANUFACTURING INDEX",
                        "BSE INDIA SECTOR LEADERS",
                        "BSE INDUSTRIALS",
                        "BSE INFORMATION TECHNOLOGY",
                        "BSE INTERNET ECONOMY",
                        "BSE METAL",
                        "BSE OIL & GAS",
                        "BSE POWER",
                        "BSE POWER & ENERGY",
                        "BSE PREMIUM CONSUMPTION",
                        "BSE PRIVATE BANKS INDEX",
                        "BSE PSU",
                        "BSE PSU BANK",
                        "BSE REALTY",
                        "BSE Services",
                        "BSE TECK",
                        "BSE Telecommunication",
                        "BSE Utilities"
                };

                for (String indexName : bseSectoralIndices) {
                    // Truncate index names longer than 50 characters (none exceed here)
                    if (indexName.length() > 50) {
                        indexName = indexName.substring(0, 50);
                    }
                    pstmt.setString(1, indexName);
                    pstmt.setString(2, "BSE");
                    pstmt.addBatch();
                }
                System.out.println("Added " + bseSectoralIndices.length + " BSE sectoral indices to batch.");

                // Execute the batch
                int[] updateCounts = pstmt.executeBatch();
                System.out.println("Inserted " + updateCounts.length + " indices successfully!");

                pstmt.close();
            }
        } catch (SQLException e) {
            System.out.println("Failed to insert indices!");
            e.printStackTrace();
        }
    }
}