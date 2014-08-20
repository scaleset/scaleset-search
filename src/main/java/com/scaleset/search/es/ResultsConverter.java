package com.scaleset.search.es;

import com.scaleset.search.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;

import java.util.ArrayList;
import java.util.List;

public class ResultsConverter<T, K> {

    private Query query;
    private SearchResponse response;
    private int totalItems;
    private List<T> items = new ArrayList<>();
    private List<AggregationResults> facets = new ArrayList<>();
    private SearchHits hits;
    private SearchMapping<T, K> mapping;

    public ResultsConverter(Query query, SearchResponse response, SearchMapping<T, K> mapping) {
        this.query = query;
        this.response = response;
        this.mapping = mapping;
    }

    protected void addDateHistogram(DateHistogram agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (DateHistogram.Bucket entry : agg.getBuckets()) {
            // TODO - Might need to change this to use getKeyAsNumber
            buckets.add(new Bucket(entry.getKey(), entry.getDocCount()));
        }
        facets.add(new AggregationResults(agg.getName(), buckets));
    }

    private void addExtendedStats(ExtendedStats agg) {
        Stats stats = new Stats(agg.getCount(), agg.getSum(), agg.getMin(), agg.getMax(), agg.getAvg(), agg.getSumOfSquares(), agg.getVariance(), agg.getStdDeviation());
        facets.add(new AggregationResults(agg.getName(), null, stats));
    }

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

    protected void addHistogram(Histogram agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (Histogram.Bucket bucket : agg.getBuckets()) {
            buckets.add(new Bucket(bucket.getKey(), bucket.getDocCount()));
        }
        facets.add(new AggregationResults(agg.getName(), buckets));
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
        facets.add(new AggregationResults(agg.getName(), buckets));
    }

    protected void addRange(Range agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (Range.Bucket bucket : agg.getBuckets()) {
            String label = bucket.getKeyAsText() + " TO " + bucket.getKeyAsText();
            buckets.add(new Bucket(label, bucket.getDocCount()));
        }
        facets.add(new AggregationResults(agg.getName(), buckets));
    }

    private void addStats(org.elasticsearch.search.aggregations.metrics.stats.Stats agg) {
        Stats stats = new Stats(agg.getCount(), agg.getSum(), agg.getMin(), agg.getMax(), agg.getAvg());
        facets.add(new AggregationResults(agg.getName(), null, stats));
    }

    protected void addTerms(Terms agg) {
        List<Bucket> buckets = new ArrayList<>();
        for (Terms.Bucket bucket : agg.getBuckets()) {
            buckets.add(new Bucket(bucket.getKey(), bucket.getDocCount()));
        }
        facets.add(new AggregationResults(agg.getName(), buckets));
    }

    public Results<T> convert() throws Exception {
        initialize();
        addItems();
        addFacets();
        Results<T> results = new Results<T>(query, facets, items, totalItems);
        return results;
    }

    protected void initialize() {
        hits = response.getHits();
        totalItems = (int) hits.getTotalHits();
    }

}
