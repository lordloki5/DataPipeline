package org.example;

import okhttp3.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NSEAPIExample {
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .cookieJar(new SimpleCookieJar()) // Custom CookieJar to store cookies
            .build();
    private static final String BASE_URL = "https://www.nseindia.com";
    private static final String API_URL = "https://www.nseindia.com/api/historical/indicesHistory?indexType=NIFTY%20BANK&from=01-05-2024&to=07-03-2025";

//    public static void main(String[] args) {
//        try {
//            // Step 1: Initialize session by hitting the homepage
//            initializeSession();
//
//            // Step 2: Fetch historical data
//            String response = fetchHistoricalData(API_URL);
//            System.out.println(response);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void initializeSession() throws IOException {
        Request request = new Request.Builder()
                .url(BASE_URL)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .header("Referer", "https://www.nseindia.com/")
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to initialize session: " + response.code());
            }
            // Cookies are automatically stored in the CookieJar
            System.out.println("Session initialized. Cookies stored.");
        }
    }

    public static String fetchHistoricalData(String apiUrl) throws IOException {
//        initializeSession();
        Request request = new Request.Builder()
                .url(apiUrl)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
                .header("Accept", "application/json")
                .header("Referer", "https://www.nseindia.com/market-data/historical-data")
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 401) {
                System.out.println("401 Unauthorized detected. Refreshing session...");
                initializeSession(); // Retry session initialization
                return fetchHistoricalData(apiUrl); // Retry the request
            }
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response.code());
            }
            return response.body().string();
        }
    }

    // Simple CookieJar implementation to store and reuse cookies
    static class SimpleCookieJar implements CookieJar {
        private final Map<String, List<Cookie>> cookieStore = new HashMap<>();

        @Override
        public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
            cookieStore.put(url.host(), cookies);
        }

        @Override
        public List<Cookie> loadForRequest(HttpUrl url) {
            List<Cookie> cookies = cookieStore.get(url.host());
            return cookies != null ? cookies : new ArrayList<>();
        }
    }
}