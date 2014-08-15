package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class Aggregation {

    private String name;
    private String type;

    // properties not specified as members
    private Map<String, Object> properties = new HashMap<>();

    public Aggregation() {
    }

    public Aggregation(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }


    public String getType() {
        return type;
    }

    public String getString(String key) {
        return (String) properties.get(key);
    }

    @JsonAnySetter
    public Aggregation property(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    public void setType(String type) {
        this.type = type;
    }
}
