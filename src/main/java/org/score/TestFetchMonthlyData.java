package org.score;

import java.sql.*;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;

public class TestFetchMonthlyData {
    // Database connection URL
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";

    // Modified fetchMonthlyData function without QUALIFY
    public static Map<String, Candle> fetchMonthlyData(int indexId, LocalDate endDate) throws SQLException {
        Map<String, Candle> candles = new TreeMap<>();
        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            String sql = "WITH monthly_data AS ( " +
                    "    SELECT EXTRACT(YEAR FROM trade_date) AS year, " +
                    "           EXTRACT(MONTH FROM trade_date) AS month, " +
                    "           trade_date, " +
                    "           FIRST_VALUE(open_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date) ORDER BY trade_date) AS open_price, " +
                    "           MAX(high_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date)) AS high_price, " +
                    "           MIN(low_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date)) AS low_price, " +
                    "           LAST_VALUE(close_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date) ORDER BY trade_date " +
                    "               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price " +
                    "    FROM daily_ohlc " +
                    "    WHERE index_id = ? AND trade_date <= ? " +
                    ") " +
                    "SELECT year, month, open_price, high_price, low_price, close_price, trade_date AS last_date " +
                    "FROM monthly_data " +
                    "WHERE trade_date = (SELECT MAX(trade_date) FROM monthly_data md2 " +
                    "                    WHERE md2.year = monthly_data.year AND md2.month = monthly_data.month)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, indexId);
                pstmt.setTimestamp(2, Timestamp.valueOf(endDate.atTime(23, 59, 59)));
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    int year = rs.getInt("year");
                    int month = rs.getInt("month");
                    String key = year + "-" + String.format("%02d", month);
                    LocalDate tradeDate = rs.getTimestamp("last_date").toLocalDateTime().toLocalDate();
                    candles.put(key, new Candle(
                            tradeDate,
                            rs.getDouble("open_price"),
                            rs.getDouble("high_price"),
                            rs.getDouble("low_price"),
                            rs.getDouble("close_price")
                    ));
                    System.out.println("Fetched for index " + indexId + ": " + key + " -> Open: " + rs.getDouble("open_price") +
                            ", High: " + rs.getDouble("high_price") + ", Low: " + rs.getDouble("low_price") +
                            ", Close: " + rs.getDouble("close_price"));
                }
            }
        }
        return candles;
    }

    // Candle class
    public static class Candle {
        private LocalDate date;
        private double open;
        private double high;
        private double low;
        private double close;

        public Candle(LocalDate date, double open, double high, double low, double close) {
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
        }

        @Override
        public String toString() {
            return "Candle{" +
                    "date=" + date +
                    ", open=" + open +
                    ", high=" + high +
                    ", low=" + low +
                    ", close=" + close +
                    '}';
        }
    }

    // Main method to test fetchMonthlyData
    public static void main(String[] args) {
        try {
            // Test parameters
            int indexId = 40; // Using the index_id from your error message
            LocalDate endDate = LocalDate.of(2025, 3, 31); // Test up to March 31, 2025

            System.out.println("Fetching monthly OHLC data for index " + indexId + " up to " + endDate + "...");

            // Call the function
            Map<String, Candle> monthlyData = fetchMonthlyData(indexId, endDate);

            // Print the results
            System.out.println("\nResults:");
            if (monthlyData.isEmpty()) {
                System.out.println("No data found for index " + indexId + " up to " + endDate);
            } else {
                for (Map.Entry<String, Candle> entry : monthlyData.entrySet()) {
                    System.out.println(entry.getKey() + " -> " + entry.getValue());
                }
                System.out.println("Total months fetched: " + monthlyData.size());
            }

        } catch (SQLException e) {
            System.err.println("Error fetching data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}