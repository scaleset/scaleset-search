package com.scaleset.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.utils.Coerce;
import com.scaleset.utils.Extensible;

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
