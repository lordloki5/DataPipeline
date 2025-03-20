package org.example2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class HistoricalDataBse {
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";
    private static String fromDate = "01-01-2015";
    private static String toDate = "17-03-2025";
    private static final int MAX_DAYS_PER_CALL = 26; // Updated to 26 days as per BSE API limit
    private static final String[] INDEX_NAMES = {"BSE 500"};
    private static final String[] INDEX_KEYWORDS = {"BSE500"};

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

    public static void insertData(IndexDataBSE data , String indexName) throws Exception {
        String sql = "INSERT INTO daily_ohlc (trade_date, index_id, open_price, high_price, low_price, close_price) " +
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (index_id, trade_date) DO NOTHING";

        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            conn.setAutoCommit(false);
            int indexId = getOrInsertIndexId(conn, indexName, "BSE");
            java.sql.Timestamp tradeDate = java.sql.Timestamp.valueOf(data.getTdate().replace("T", " ").substring(0, 19));

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setTimestamp(1, tradeDate);
                pstmt.setInt(2, indexId);
                pstmt.setDouble(3, data.getOpen());
                pstmt.setDouble(4, data.getHigh());
                pstmt.setDouble(5, data.getLow());
                pstmt.setDouble(6, data.getClose());
                int rowsAffected = pstmt.executeUpdate();
                System.out.println("Inserted " + rowsAffected + " row(s) for " + data.getIndexName() + " on " + tradeDate);
            }
            conn.commit();
        }
    }

    public static void fetchAndInsertData(String indexName, String indexKeyword, LocalDate startDate, LocalDate endDate) throws Exception {
        DateTimeFormatter apiFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        String apiUrl = String.format(
                "https://api.bseindia.com/BseIndiaAPI/api/IndexArchDaily/w?fmdt=%s&index=%s&period=D&todt=%s",
                startDate.format(apiFormatter).replace("/", "%2F"),
                indexKeyword,
                endDate.format(apiFormatter).replace("/", "%2F")
        );

        System.out.println("Fetching data from: " + apiUrl);
        String jsonResponse = BSEAPI.fetchHistoricalData(apiUrl);
        List<IndexDataBSE> dataList = DataProcessorBSE.processBseApiResponse(jsonResponse);

        for (IndexDataBSE data : dataList) {
            data.printData();
            insertData(data,indexName);
        }
        System.out.println("Inserted " + dataList.size() + " records for " + indexName + " for range " + startDate + " to " + endDate);
    }

    public static void main(String[] args) throws Exception {
        // Initialize session to fetch and store cookies before making API requests
        BSEAPI.initializeSession();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        LocalDate startDate = LocalDate.parse(fromDate, formatter);
        LocalDate endDate = LocalDate.parse(toDate, formatter);
        long totalDays = ChronoUnit.DAYS.between(startDate, endDate) + 1; // Inclusive of end date
        System.out.println("Total days to fetch: " + totalDays);

        for (int i = 0; i < INDEX_NAMES.length; i++) {
            String indexName = INDEX_NAMES[i];
            String indexKeyword = INDEX_KEYWORDS[i];
            System.out.println("\nProcessing index: " + indexName);

            LocalDate chunkStart = startDate;
            while (chunkStart.isBefore(endDate) || chunkStart.isEqual(endDate)) {
                LocalDate chunkEnd = chunkStart.plusDays(MAX_DAYS_PER_CALL - 1); // End date is inclusive
                if (chunkEnd.isAfter(endDate)) {
                    chunkEnd = endDate; // Don’t exceed the overall end date
                }

                fetchAndInsertData(indexName, indexKeyword, chunkStart, chunkEnd);
                chunkStart = chunkEnd.plusDays(1); // Move to the next chunk
            }
        }
    }
}