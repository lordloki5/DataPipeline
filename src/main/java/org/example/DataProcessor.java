package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class DataProcessor {
    public static List<IndexDataNSE> processApiResponse(String jsonResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(jsonResponse);

        // Navigate to the "indexCloseOnlineRecords" array
        JsonNode recordsNode = rootNode.path("data").path("indexCloseOnlineRecords");
        if (recordsNode.isMissingNode() || !recordsNode.isArray()) {
            throw new IllegalArgumentException("Invalid JSON: 'indexCloseOnlineRecords' array not found");
        }

        List<IndexDataNSE> results = new ArrayList<>();
        for (JsonNode node : recordsNode) {
            if (node.get("EOD_INDEX_NAME") != null) { // Ensure it's an EOD record
                String id = node.get("_id") != null ? node.get("_id").asText() : null;
                String indexName = node.get("EOD_INDEX_NAME").asText(); // Required field
                double openIndexValue = node.get("EOD_OPEN_INDEX_VAL") != null ? node.get("EOD_OPEN_INDEX_VAL").asDouble() : 0.0;
                double highIndexValue = node.get("EOD_HIGH_INDEX_VAL") != null ? node.get("EOD_HIGH_INDEX_VAL").asDouble() : 0.0;
                double closeIndexValue = node.get("EOD_CLOSE_INDEX_VAL") != null ? node.get("EOD_CLOSE_INDEX_VAL").asDouble() : 0.0;
                double lowIndexValue = node.get("EOD_LOW_INDEX_VAL") != null ? node.get("EOD_LOW_INDEX_VAL").asDouble() : 0.0;
                String eodTimestamp = node.get("EOD_TIMESTAMP") != null ? node.get("EOD_TIMESTAMP").asText() : null;
                String timestamp = node.get("TIMESTAMP") != null ? node.get("TIMESTAMP").asText() : null;

                results.add(new IndexDataNSE(id, indexName, openIndexValue, highIndexValue,
                        closeIndexValue, lowIndexValue, eodTimestamp, timestamp));
            }
        }

        if (results.isEmpty()) {
            throw new IllegalArgumentException("No valid EOD records found in the JSON response");
        }
        return results;
    }
}