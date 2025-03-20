package org.example2;

import okhttp3.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BSEAPI {
    // OkHttpClient with CookieJar for persistent cookie management
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .cookieJar(new SimpleCookieJar())
            .build();
    private static final String BASE_URL = "https://www.bseindia.com";

    /**
     * Initializes the session by making a request to the BSE homepage and storing cookies.
     */
    public static void initializeSession() throws IOException {
        Request request = new Request.Builder()
                .url(BASE_URL)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                .header("Referer", "https://www.bseindia.com/")
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to initialize session: " + response.code());
            }
            System.out.println("Session initialized. Cookies stored.");
        }
    }

    /**
     * Fetches historical data from the BSE API using the initialized session's cookies.
     */
    public static String fetchHistoricalData(String apiUrl) throws IOException {
        Request request = new Request.Builder()
                .url(apiUrl)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
                .header("Accept", "application/json")
                .header("Referer", "https://www.bseindia.com/markets/equity/EQReports/index.aspx")
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 401) {
                System.out.println("401 Unauthorized detected. Refreshing session...");
                initializeSession(); // Reinitialize session on 401 error
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