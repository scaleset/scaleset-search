package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.scaleset.utils.Extensible;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder({"label", "key", "count", "stats", "aggs"})
public final class Bucket extends Extensible {

    private long count;
    private String label;
    private Object key;
    private Stats stats;
    private Map<String, AggregationResults> aggs = new HashMap<>();

    public Bucket() {
    }

    public Bucket(Object key, long count) {
        this.key = key;
        this.count = count;
        this.label = null;
    }

    public Bucket(Object key, long count, String label) {
        this.key = key;
        this.count = count;
        this.label = label;
    }

    public Bucket(Object key, long count, String label, Stats stats) {
        this.key = key;
        this.count = count;
        this.label = label;
        this.stats = stats;
    }

    public Map<String, AggregationResults> getAggs() {
        return aggs;
    }

    public long getCount() {
        return count;
    }

    public String getLabel() {
        return label;
    }

    public Stats getStats() {
        return stats;
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public Object getKey() {
        return key;
    }

    public void setAggs(Map<String, AggregationResults> aggs) {
        this.aggs = aggs;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setKey(Object key) {
        this.key = key;
    }
}