package com.scaleset.search;

import com.scaleset.utils.Extensible;

import java.util.Map;

public class Aggregation extends Extensible {

    private String name;
    private String type;

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

    public String getName() {
        return name;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
