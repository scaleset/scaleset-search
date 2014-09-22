package com.scaleset.search.es;

import com.scaleset.search.*;
import com.scaleset.search.es.agg.AggregationConverter;
import com.scaleset.search.es.agg.AggregationResultsConverter;
import com.scaleset.search.es.agg.TermAggregationConverter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;

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
    private Map<String, AggregationResultsConverter> aggConverters = new HashMap<>();

    public ResultsConverter(Query query, SearchResponse response, SearchMapping<T, K> mapping) {
        this.query = query;
        this.response = response;
        this.mapping = mapping;
        registerDefaultConverters();
    }

    protected void addDateHistogram(DateHistogram agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (DateHistogram.Bucket entry : agg.getBuckets()) {
            // TODO - Might need to change this to use getKeyAsNumber
            buckets.add(new Bucket(entry.getKey(), entry.getDocCount()));
        }
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), buckets));
    }

    private void addExtendedStats(ExtendedStats agg) {
        Stats stats = new Stats(agg.getCount(), agg.getSum(), agg.getMin(), agg.getMax(), agg.getAvg(), agg.getSumOfSquares(), agg.getVariance(), agg.getStdDeviation());
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), null, stats));
    }

    /*
    protected void addFacets() {
        Aggregations aggs = response.getAggregations();
        if (aggs != null) {
            for (Aggregation agg : aggs) {
                if (agg instanceof Terms) {
                    addTerms((Terms) agg);
                } else if (agg instanceof Range) {
                    addRange((Range) agg);
                } else if (agg instanceof Histogram) {
                    addHistogram((Histogram) agg);
                } else if (agg instanceof DateHistogram) {
                    addDateHistogram((DateHistogram) agg);
                } else if (agg instanceof org.elasticsearch.search.aggregations.metrics.stats.Stats) {
                    addStats((org.elasticsearch.search.aggregations.metrics.stats.Stats) agg);
                } else if (agg instanceof ExtendedStats) {
                    addExtendedStats((ExtendedStats) agg);
                } else if (agg instanceof Filter) {
                    addQuery((Filter) agg);
                }
            }
        }
    }
    */

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

    protected void addHistogram(Histogram agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (Histogram.Bucket bucket : agg.getBuckets()) {
            buckets.add(new Bucket(bucket.getKey(), bucket.getDocCount()));
        }
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), buckets));
    }

    protected void addItems() throws Exception {
        for (SearchHit hit : hits) {
            try {
                String source = hit.getSourceAsString();
                String id = hit.getId();
                items.add(mapping.fromDocument(id, source));
            } catch (Exception e) {
            }
        }
    }

    protected void addQuery(Filter agg) {
        List<Bucket> buckets = new ArrayList<>();
        buckets.add(new Bucket(agg.getName(), agg.getDocCount()));
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), buckets));
    }

    protected void addRange(Range agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (Range.Bucket bucket : agg.getBuckets()) {
            String label = bucket.getKeyAsText() + " TO " + bucket.getKeyAsText();
            buckets.add(new Bucket(label, bucket.getDocCount()));
        }
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), buckets));
    }

    private void addStats(org.elasticsearch.search.aggregations.metrics.stats.Stats agg) {
        Stats stats = new Stats(agg.getCount(), agg.getSum(), agg.getMin(), agg.getMax(), agg.getAvg());
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), null, stats));
    }

    protected void addTerms(Terms agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (Terms.Bucket bucket : agg.getBuckets()) {
            buckets.add(new Bucket(bucket.getKey(), bucket.getDocCount()));
        }
        aggs.put(agg.getName(), new AggregationResults(agg.getName(), buckets));
    }

    public Results<T> convert() throws Exception {
        initialize();
        addItems();
        //addFacets();
        addAggregations();
        Results<T> results = new Results<T>(query, aggs, items, totalItems);
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
    }

}
