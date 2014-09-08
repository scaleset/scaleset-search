package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.scaleset.utils.Extensible;

@JsonInclude(Include.NON_EMPTY)
public final class Bucket extends Extensible {

    public long count;
    public String label;
    public Object key;
    public Stats stats;

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

    public long getCount() {
        return count;
    }

    public String getLabel() {
        return label;
    }

    public Stats getStats() {
        return stats;
    }

    public Object getKey() {
        return key;
    }

}