package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "buckets", "stats"})
public class AggregationResults {

    private List<Bucket> buckets = new ArrayList<Bucket>();
    private Stats stats;
    private String name;

    public AggregationResults() {
    }

    public AggregationResults(String name, List<Bucket> buckets) {
        this(name, buckets, null);
    }

    public AggregationResults(String name, List<Bucket> buckets, Stats stats) {
        this.name = name;
        if (buckets != null) {
            this.buckets.addAll(buckets);
        }
        this.stats = stats;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public String getName() {
        return name;
    }

    public Stats getStats() {
        return stats;
    }

}
