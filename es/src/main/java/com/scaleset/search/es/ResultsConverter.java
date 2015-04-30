package com.scaleset.search.es;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Query;
import com.scaleset.search.Results;
import com.scaleset.search.es.agg.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultsConverter<T, K> {

    private Query query;
    private SearchResponse response;
    private int totalItems;
    private List<T> items = new ArrayList<>();
    private Map<String, AggregationResults> aggs = new HashMap<>();
    private SearchHits hits;
    private SearchMapping<T, K> mapping;
    private Map<String, Object> headers = new HashMap<>();
    private Map<String, AggregationResultsConverter> aggConverters = new HashMap<>();

    public ResultsConverter(Query query, SearchResponse response, SearchMapping<T, K> mapping) {
        this.query = query;
        this.response = response;
        this.mapping = mapping;
        registerDefaultConverters();
    }

    protected void addAggregations() {
        for (String name : query.getAggs().keySet()) {
            Aggregation agg = query.getAggs().get(name);
            addAggregationResults(agg);
        }
    }

    protected void addAggregationResults(Aggregation agg) {
        String type = agg.getType();
        String name = agg.getName();
        AggregationResultsConverter converter = aggConverters.get(type);
        if (converter != null) {
            AggregationResults results = convertResults(agg, response.getAggregations());
            if (results != null) {
                aggs.put(name, results);
            }
        }
    }

    public AggregationResults convertResults(Aggregation agg, Aggregations aggs) {
        AggregationResults results = null;
        String type = agg.getType();
        AggregationResultsConverter converter = aggConverters.get(type);
        if (converter != null) {
            results = converter.convertResult(this, agg, aggs);
        }
        return results;
    }

    protected void addItems() throws Exception {
        for (SearchHit hit : hits) {
            try {
                String source = hit.getSourceAsString();
                if (source != null) {
                    String id = hit.getId();
                    items.add(mapping.fromDocument(id, source));
                } else {
                    items.add(mapping.fromSearchHit(hit));
                }
            } catch (Exception e) {
            }
        }
    }

    protected void addHeaders() {
        headers.put("took", response.getTookInMillis());
        if (response.getScrollId() != null) {
            headers.put("scrollId", response.getScrollId());
        }
    }

    public Results<T> convert() throws Exception {
        initialize();
        addItems();
        addAggregations();
        addHeaders();
        Results<T> results = new Results<T>(query, aggs, items, totalItems, null, headers);
        return results;
    }

    protected void initialize() {
        hits = response.getHits();
        totalItems = (int) hits.getTotalHits();
    }

    public ResultsConverter register(String type, AggregationResultsConverter converter) {
        aggConverters.put(type, converter);
        return this;
    }

    protected void registerDefaultConverters() {
        register("terms", new TermAggregationConverter());
        register("filter", new FilterAggregationConverter());
        register("range", new RangeAggregationConverter());
        register("stats", new StatsAggregationConverter());
        register("geohash_grid", new GeoHashGridAggregationConverter());
        register("geohash_grid_stats", new GeoHashGridStatsAggregationConverter());
    }

}
