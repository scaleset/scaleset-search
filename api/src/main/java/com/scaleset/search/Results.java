package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.vividsolutions.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"headers", "query", "totalItems", "aggs", "bbox", "items"})
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Results<T> {

    private Map<String, AggregationResults> aggs = new HashMap<>();
    private List<T> items = new ArrayList<>();
    private Query query;
    private Integer totalItems = 0;
    private Envelope bbox;
    private Map<String, Object> headers = new HashMap<String, Object>();
    private List<String> fields = new ArrayList<>();

    /* For Jackson */
    protected Results() {
    }

    public Results(Query query, Map<String, AggregationResults> aggs, List<T> items, Integer totalItems) {
        this(query, aggs, items, totalItems, null, null, null);
    }

    public Results(Query query, Map<String, AggregationResults> aggs, List<T> items, Integer totalItems, Envelope bbox, Map<String, Object> headers, List<String> fields) {
        this.query = (query != null && query.getEcho()) ? query : null;
        if (aggs != null) {
            this.aggs.putAll(aggs);
        }
        this.items.addAll(items);
        this.totalItems = totalItems;
        this.bbox = bbox;
        if (headers != null) {
            this.headers.putAll(headers);
        }
        if (fields != null) {
            this.fields.addAll(fields);
        }
    }

    public Results(Query query, List<T> items) {
        this.query = query;
        this.items.addAll(items);
    }

    public AggregationResults getAgg(String name) {
        return aggs.get(name);
    }

    public Map<String, AggregationResults> getAggs() {
        return aggs;
    }

    public Envelope getBbox() {
        return bbox;
    }

    public List<String> getFields() {
        return fields;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public List<T> getItems() {
        return items;
    }

    public Query getQuery() {
        return query;
    }

    public Integer getTotalItems() {
        return totalItems;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}
