package org.example;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class HistoricalDataNse {
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";
    private static String fromDate = "01-03-2025"; // Adjust to earliest relevant date
    private static String toDate = "11-03-2025";   // Current date or your end date
    private static final int MAX_DAYS_PER_CALL = 299; // Less than 300 days
    // Array of index names to fetch
    private static final String[] INDEX_NAMES = {
            "NIFTY BANK",
            "NIFTY ENERGY",
            "NIFTY IT",
            "NIFTY FMCG" // Add more indices as needed
    };

    // Get or insert index_id for the given index_name
    private static int getOrInsertIndexId(Connection conn, String indexName, String exchange) throws Exception {
        String selectSql = "SELECT index_id FROM indices WHERE index_name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
            pstmt.setString(1, indexName);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("index_id");
            }
        }

        String insertSql = "INSERT INTO indices (index_name, exchange) VALUES (?, ?) RETURNING index_id";
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            pstmt.setString(1, indexName);
            pstmt.setString(2, exchange);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("index_id");
            }
        }
        throw new RuntimeException("Failed to insert or retrieve index_id for " + indexName);
    }

    public static void insertData(IndexDataNSE data) throws Exception {
        String sql = "INSERT INTO daily_ohlc (trade_date, index_id, open_price, high_price, low_price, close_price) " +
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (index_id, trade_date) DO NOTHING";

        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            conn.setAutoCommit(false);

            int indexId = getOrInsertIndexId(conn, data.getIndexName(), "NSE");
            ZonedDateTime tradeDate = ZonedDateTime.parse(data.getTimestamp(), DateTimeFormatter.ISO_DATE_TIME);

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setTimestamp(1, java.sql.Timestamp.from(tradeDate.toInstant()));
                pstmt.setInt(2, indexId);
                pstmt.setDouble(3, data.getOpenIndexValue());
                pstmt.setDouble(4, data.getHighIndexValue());
                pstmt.setDouble(5, data.getLowIndexValue());
                pstmt.setDouble(6, data.getCloseIndexValue());

                int rowsAffected = pstmt.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Data inserted for " + data.getIndexName() + " on " + tradeDate);
                } else {
                    System.out.println("Data already exists for " + data.getIndexName() + " on " + tradeDate);
                }
            }

            conn.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert data into TimescaleDB: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        try {
            NSEAPIExample.initializeSession();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
            LocalDate startDate = LocalDate.parse(fromDate, formatter);
            LocalDate endDate = LocalDate.parse(toDate, formatter);
            long totalDays = ChronoUnit.DAYS.between(startDate, endDate);
            System.out.println("Total days to fetch per index: " + totalDays);

            // Loop through each index
            for (String indexName : INDEX_NAMES) {
                System.out.println("\nProcessing index: " + indexName);
                LocalDate currentStart = startDate;

                while (currentStart.isBefore(endDate) || currentStart.isEqual(endDate)) {
                    LocalDate currentEnd = currentStart.plusDays(MAX_DAYS_PER_CALL - 1);
                    if (currentEnd.isAfter(endDate)) {
                        currentEnd = endDate;
                    }

                    String chunkFromDate = currentStart.format(formatter);
                    String chunkToDate = currentEnd.format(formatter);
                    String apiUrl = String.format(
                            "https://www.nseindia.com/api/historical/indicesHistory?indexType=%s&from=%s&to=%s",
                            indexName.replace(" ", "%20"), // Replace space with %20 for URL encoding
                            chunkFromDate,
                            chunkToDate
                    );

                    System.out.println("Fetching data for " + indexName + " from " + chunkFromDate + " to " + chunkToDate + "...");
                    String jsonResponse = NSEAPIExample.fetchHistoricalData(apiUrl);
                    System.out.println("Raw JSON Response: " + jsonResponse);

                    // Process all EOD records for this chunk
                    List<IndexDataNSE> processedDataList = DataProcessor.processApiResponse(jsonResponse);

                    // Insert each record into TimescaleDB
                    for (IndexDataNSE data : processedDataList) {
                        data.printData(); // Assumes IndexDataNSE has a printData() method
                        insertData(data);
                    }
                    System.out.println("Inserted " + processedDataList.size() + " records for " + indexName + " from " + chunkFromDate + " to " + chunkToDate);

                    currentStart = currentEnd.plusDays(1);
                }
                System.out.println("Completed processing for " + indexName);
            }

            System.out.println("All-time data fetch and insertion completed for all indices.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Optional: Method to set custom date range
    public static void setDateRange(String newFromDate, String newToDate) {
        fromDate = newFromDate;
        toDate = newToDate;
    }
}