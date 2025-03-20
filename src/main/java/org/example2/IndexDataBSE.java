package org.example2;


public class IndexDataBSE {
    private String indexName;
    private String tdate;
    private double open;
    private double high;
    private double low;
    private double close;

    public IndexDataBSE(String indexName, String tdate, double open, double high, double low, double close) {
        this.indexName = indexName;
        this.tdate = tdate;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
    }

    public String getIndexName() { return indexName; }
    public String getTdate() { return tdate; }
    public double getOpen() { return open; }
    public double getHigh() { return high; }
    public double getLow() { return low; }
    public double getClose() { return close; }

    public void printData() {
        System.out.println("Index: " + indexName + ", Date: " + tdate + ", Open: " + open +
                ", High: " + high + ", Low: " + low + ", Close: " + close);
    }
}