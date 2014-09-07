package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.utils.Coerce;
import com.scaleset.utils.Extensible;

import java.util.Map;

@JsonPropertyOrder({"type", "name"})
public class Filter extends Extensible {

    private String name;
    private String type;

    public Filter() {
        super(new Coerce(new ObjectMapper().registerModule(new GeoJsonModule())));
    }

    public Filter(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public Filter(String name, String type, Map<String, Object> properties) {
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
