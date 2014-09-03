package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class Filter {

    private String name;
    private String type;

    // properties not specified as members
    private Map<String, Object> properties = new HashMap<>();

    public Filter() {
    }

    public Filter(String name, String type) {
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

    public Integer getInteger(String key) {
        return ((Integer) properties.get(key));
    }

    @JsonAnySetter
    public Filter property(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    public void setType(String type) {
        this.type = type;
    }
}
