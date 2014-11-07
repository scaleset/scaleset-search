package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.scaleset.utils.Extensible;

import java.util.HashMap;
import java.util.Map;

public class Aggregation extends Extensible {

    @JsonIgnore
    private String name;
    private String type;

    private Map<String, Aggregation> aggs = new HashMap<String, Aggregation>();

    public Aggregation() {
    }

    public Aggregation(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public Aggregation(String name, String type, Map<String, Object> properties) {
        this.name = name;
        this.type = type;
        for (String key : properties.keySet()) {
            put(key, properties.get(key));
        }
    }

    public Aggregation(String name, String type, Map<String, Object> properties, Map<String, Aggregation> aggs) {
        this.name = name;
        this.type = type;
        for (String key : properties.keySet()) {
            put(key, properties.get(key));
        }
        if (aggs != null) {
            for (String subName : aggs.keySet()) {
                Aggregation subAgg = aggs.get(subName);
                this.aggs.put(subName, new Aggregation(subName, subAgg.type, subAgg.anyGetter(), subAgg.aggs));
            }
        }
    }

    public Map<String, Aggregation> getAggs() {
        return aggs;
    }

    public String getName() {
        return name;
    }


    public String getType() {
        return type;
    }

    public void setAggs(Map<String, Aggregation> aggs) {
        this.aggs = aggs;
    }

    public void setType(String type) {
        this.type = type;
    }
}
