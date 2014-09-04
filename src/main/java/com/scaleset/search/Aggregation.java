package com.scaleset.search;

import com.scaleset.utils.Extensible;

public class Aggregation extends Extensible {

    private String name;
    private String type;

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

    public void setType(String type) {
        this.type = type;
    }
}
