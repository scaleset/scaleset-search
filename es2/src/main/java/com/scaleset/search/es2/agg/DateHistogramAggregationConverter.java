package com.scaleset.search.es2.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es2.QueryConverter;
import com.scaleset.search.es2.ResultsConverter;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

import java.util.ArrayList;
import java.util.List;

public class DateHistogramAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

        DateHistogramBuilder result = AggregationBuilders.dateHistogram(getName(aggregation));
        String field = aggregation.getString("field");
        String interval = aggregation.getString("interval");
        String format = aggregation.getString("format");
        if (field != null) {
            result.field(field);
        }
        if (interval != null) {
            DateHistogramInterval i = new DateHistogramInterval(interval);
            result.interval(i);
        }
        if (format != null) {
            result.format(format);
        }
        // not sure we really want to support sub-aggs, but we will
        addSubAggs(queryConverter, aggregation, result);
        return result;
    }

    @Override
    public AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation
            aggregation, Aggregations aggs) {
        String name = aggregation.getName();
        AggregationResults result = null;
        // TODO: Do we need to make sure this was a date histogram?
        if (aggs.get(name) instanceof Histogram) {
            Histogram grid = aggs.get(name);
            List<Bucket> buckets = new ArrayList<>();
            grid.getBuckets().stream().filter(bucket -> bucket.getDocCount() > 0).forEach(bucket -> {
                Bucket b = new Bucket(bucket.getKeyAsString(), bucket.getDocCount());
                buckets.add(b);
                for (Aggregation subAgg : aggregation.getAggs().values()) {
                    AggregationResults subResults = resultsConverter.convertResults(subAgg, bucket.getAggregations());
                    if (subResults != null) {
                        b.getAggs().put(subAgg.getName(), subResults);
                    }
                }
            });
            result = new AggregationResults(name, buckets);
        }
        return result;
    }
}
