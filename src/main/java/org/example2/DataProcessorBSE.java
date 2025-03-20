package org.example2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;

public class DataProcessorBSE {
    public static List<IndexDataBSE> processBseApiResponse(String jsonResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(jsonResponse);
        JsonNode tableNode = rootNode.path("Table");

        if (tableNode.isMissingNode() || !tableNode.isArray()) {
            throw new IllegalArgumentException("Invalid JSON: 'Table' array not found");
        }

        List<IndexDataBSE> results = new ArrayList<>();
        for (JsonNode node : tableNode) {
            String indexName = node.get("I_name").asText();
            String tdate = node.get("tdate").asText();
            double open = node.get("I_open").asDouble();
            double high = node.get("I_high").asDouble();
            double low = node.get("I_low").asDouble();
            double close = node.get("I_close").asDouble();
            results.add(new IndexDataBSE(indexName, tdate, open, high, low, close));
        }
        return results;
    }
}