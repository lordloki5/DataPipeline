package org.example;

public class IndexDataNSE {
    private String id;
    private String indexName;
    private double openIndexValue;
    private double highIndexValue;
    private double closeIndexValue;
    private double lowIndexValue;
    private String eodTimestamp; // e.g., "07-MAR-2024"
    private String timestamp;   // e.g., "2024-03-06T18:30:00.000Z"

    // Constructor
    public IndexDataNSE(String id, String indexName, double openIndexValue, double highIndexValue,
                        double closeIndexValue, double lowIndexValue, String eodTimestamp, String timestamp) {
        this.id = id;
        this.indexName = indexName;
        this.openIndexValue = openIndexValue;
        this.highIndexValue = highIndexValue;
        this.closeIndexValue = closeIndexValue;
        this.lowIndexValue = lowIndexValue;
        this.eodTimestamp = eodTimestamp;
        this.timestamp = timestamp;
    }
    public void printData() {
        System.out.println("Index Data:");
        System.out.println("ID: " + id);
        System.out.println("Index Name: " + indexName);
        System.out.println("Open Value: " + openIndexValue);
        System.out.println("High Value: " + highIndexValue);
        System.out.println("Close Value: " + closeIndexValue);
        System.out.println("Low Value: " + lowIndexValue);
        System.out.println("EOD Timestamp: " + eodTimestamp);
        System.out.println("Timestamp: " + timestamp);
        System.out.println("------------------------");
    }
    // Getters
    public String getId() { return id; }
    public String getIndexName() { return indexName; }
    public double getOpenIndexValue() { return openIndexValue; }
    public double getHighIndexValue() { return highIndexValue; }
    public double getCloseIndexValue() { return closeIndexValue; }
    public double getLowIndexValue() { return lowIndexValue; }
    public String getEodTimestamp() { return eodTimestamp; }
    public String getTimestamp() { return timestamp; }
}