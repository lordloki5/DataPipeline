package org.score;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class CalculateMonthlyCandleScores {
    static final String CONN_URL = "jdbc:postgresql://mlr70xcmp1.lad5559etb.tsdb.cloud.timescale.com:32285/tsdb?sslmode=require&user=tsdbadmin&password=Timescale##1234";
    static final int SECTORAL_INDEX_ID = 40; // Your sectoral index
    static final int NIFTY50_INDEX_ID = 61;  // NIFTY50 benchmark
    static final int BSE500_INDEX_ID = 62;   // BSE500 benchmark
    static final LocalDate SCORE_DATE = LocalDate.of(2025, 3, 1); // Score for March 2025

    static class Candle {
        LocalDate tradeDate;
        double open, high, low, close;
        String tag;
        Integer n1, n2, n3, b1, b2, b3;

        Candle(LocalDate tradeDate, double open, double high, double low, double close) {
            this.tradeDate = tradeDate;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
        }

        @Override
        public String toString() {
            return String.format("Date: %s, Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f, Tag: %s, n1: %s, n2: %s, n3: %s, b1: %s, b2: %s, b3: %s",
                    tradeDate, open, high, low, close, tag,
                    n1 != null ? n1 : "null", n2 != null ? n2 : "null", n3 != null ? n3 : "null",
                    b1 != null ? b1 : "null", b2 != null ? b2 : "null", b3 != null ? b3 : "null");
        }
    }
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
    // Fetch last day of each month’s data up to a cutoff date
    public static Map<String, Candle> fetchMonthlyData2(int indexId, LocalDate endDate) throws SQLException {
        Map<String, Candle> candles = new TreeMap<>();
        try (Connection conn = DriverManager.getConnection(CONN_URL)) {
            String sql = "SELECT d.trade_date, d.open_price, d.high_price, d.low_price, d.close_price " +
                    "FROM daily_ohlc d " +
                    "INNER JOIN (SELECT EXTRACT(YEAR FROM trade_date) AS year, " +
                    "                   EXTRACT(MONTH FROM trade_date) AS month, " +
                    "                   MAX(trade_date) AS max_date " +
                    "            FROM daily_ohlc " +
                    "            WHERE index_id = ? AND trade_date <= ? " +
                    "            GROUP BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date)) m " +
                    "ON d.trade_date = m.max_date AND d.index_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, indexId);
                pstmt.setTimestamp(2, Timestamp.valueOf(endDate.atTime(23, 59, 59)));
                pstmt.setInt(3, indexId);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    LocalDate tradeDate = rs.getTimestamp("trade_date").toLocalDateTime().toLocalDate();
                    String key = tradeDate.getYear() + "-" + String.format("%02d", tradeDate.getMonthValue());
                    candles.put(key, new Candle(
                            tradeDate,
                            rs.getDouble("open_price"),
                            rs.getDouble("high_price"),
                            rs.getDouble("low_price"),
                            rs.getDouble("close_price")
                    ));
                    System.out.println("Fetched for index " + indexId + ": " + key + " -> " + tradeDate);
                }
            }
        }
        return candles;
    }

    // Calculate ratio data, ensuring alignment by month
    public static List<Candle> getRatioData(Map<String, Candle> sectoral, Map<String, Candle> benchmark) {
        List<Candle> ratioCandles = new ArrayList<>();
        for (String month : sectoral.keySet()) {
            if (benchmark.containsKey(month)) {
                Candle s = sectoral.get(month);
                Candle b = benchmark.get(month);
                ratioCandles.add(new Candle(
                        s.tradeDate,
                        s.open / b.open,
                        s.high / b.high,
                        s.low / b.low,
                        s.close / b.close
                ));
            }
        }
        return ratioCandles;
    }

    // Tag candles based on Python logic
    public static void tagCandles(List<Candle> candles) {
        String prevTag = "Bullish";
        for (int i = 0; i < candles.size(); i++) {
            Candle curr = candles.get(i);
            if (i == 0) {
                curr.tag = "Bullish";
            } else {
                Candle prev = candles.get(i - 1);
                double prevClose = prev.close, prevOpen = prev.open, currClose = curr.close;

                if (currClose > prevClose) { // Green candle
                    if (prevTag.equals("Highly Bullish") || prevTag.equals("Bullish")) {
                        curr.tag = prevClose >= prevOpen ? "Highly Bullish" :
                                (currClose > prevOpen ? "Highly Bullish" : "Bullish");
                    } else {
                        curr.tag = prevClose > prevOpen ? "Bullish" :
                                (currClose > prevOpen ? "Bullish" : "Bearish");
                    }
                } else if (currClose < prevClose) { // Red candle
                    if (prevTag.equals("Highly Bullish") || prevTag.equals("Bullish")) {
                        curr.tag = prevClose >= prevOpen ? (currClose > prevOpen ? "Bullish" : "Bearish") : "Bearish";
                    } else {
                        curr.tag = prevClose > prevOpen ? (currClose > prevOpen ? "Bearish" : "Highly Bearish") : "Highly Bearish";
                    }
                } else { // Neutral candle
                    if (prevTag.equals("Highly Bullish") || prevTag.equals("Bullish")) {
                        curr.tag = prevClose >= prevOpen ? prevTag : "Bullish";
                    } else {
                        curr.tag = prevClose < prevOpen ? "Bearish" : prevTag;
                    }
                }
            }
            prevTag = curr.tag;
        }
    }

    // Calculate scores for n and b types with lookbacks
    public static void calculateScores(List<Candle> nCandles, List<Candle> bCandles) {
        int[] nScores = new int[nCandles.size()];
        int[] bScores = new int[bCandles.size()];
        for (int i = 0; i < nCandles.size(); i++) {
            nScores[i] = scoreForTag(nCandles.get(i).tag);
            bScores[i] = scoreForTag(bCandles.get(i).tag);
        }
        for (int i = 0; i < nCandles.size(); i++) {
            if (i >= 1) nCandles.get(i).n1 = nScores[i - 1];
            if (i >= 2) nCandles.get(i).n2 = nScores[i - 2] + nScores[i - 1];
            if (i >= 3) nCandles.get(i).n3 = nScores[i - 3] + nScores[i - 2] + nScores[i - 1];
            if (i >= 1) bCandles.get(i).b1 = bScores[i - 1];
            if (i >= 2) bCandles.get(i).b2 = bScores[i - 2] + bScores[i - 1];
            if (i >= 3) bCandles.get(i).b3 = bScores[i - 3] + bScores[i - 2] + bScores[i - 1];
        }
    }

    private static int scoreForTag(String tag) {
        switch (tag) {
            case "Highly Bullish": return 2;
            case "Bullish": return 1;
            case "Bearish": return -1;
            case "Highly Bearish": return -2;
            default: return 0;
        }
    }

    public static void main(String[] args) {
        try {
            LocalDate cutoff = SCORE_DATE.withDayOfMonth(1).minusDays(1); // Last day of Feb (2025-02-28)
            System.out.println("Cutoff date: " + cutoff);

            Map<String, Candle> sectoralData = fetchMonthlyData(SECTORAL_INDEX_ID, cutoff);
            Map<String, Candle> niftyData = fetchMonthlyData(NIFTY50_INDEX_ID, cutoff);
            Map<String, Candle> bse500Data = fetchMonthlyData(BSE500_INDEX_ID, cutoff);

            for (Map.Entry<String, Candle> entry : sectoralData.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
            System.out.println("Sectoral data size: " + sectoralData.size());
            System.out.println("Nifty data size: " + niftyData.size());
            System.out.println("BSE500 data size: " + bse500Data.size());

            if (sectoralData.isEmpty() || niftyData.isEmpty() || bse500Data.isEmpty()) {
                System.out.println("Insufficient data for calculation.");
                return;
            }

            List<Candle> nRatio = getRatioData(sectoralData, niftyData);
            List<Candle> bRatio = getRatioData(sectoralData, bse500Data);

            if (nRatio.isEmpty() || bRatio.isEmpty()) {
                System.out.println("No matching months for ratio calculation.");
                return;
            }

            tagCandles(nRatio);
            tagCandles(bRatio);

            calculateScores(nRatio, bRatio);

            System.out.println("Scores for " + SCORE_DATE + " (based on data up to " + cutoff + "):");
            Candle lastN = nRatio.get(nRatio.size() - 1);
            Candle lastB = bRatio.get(bRatio.size() - 1);
            System.out.println("N Scores: n1=" + lastN.n1 + ", n2=" + lastN.n2 + ", n3=" + lastN.n3);
            System.out.println("B Scores: b1=" + lastB.b1 + ", b2=" + lastB.b2 + ", b3=" + lastB.b3);

            System.out.println("\nN Ratio Data:");
            nRatio.forEach(System.out::println);
            System.out.println("\nB Ratio Data:");
            bRatio.forEach(System.out::println);

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}