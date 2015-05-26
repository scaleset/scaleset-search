package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;

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
            DateHistogram.Interval i = new DateHistogram.Interval(interval);
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
        if (aggs.get(name) instanceof DateHistogram) {
            DateHistogram grid = (DateHistogram) (aggs.get(name));
            List<Bucket> buckets = new ArrayList<>();
            for (DateHistogram.Bucket bucket : grid.getBuckets()) {
                if (bucket.getDocCount() > 0) {
                    Bucket b = new Bucket(bucket.getKeyAsText().toString(), bucket.getDocCount());
                    buckets.add(b);
                    for (Aggregation subAgg : aggregation.getAggs().values()) {
                        AggregationResults subResults = resultsConverter.convertResults(subAgg, bucket.getAggregations());
                        if (subResults != null) {
                            b.getAggs().put(subAgg.getName(), subResults);
                        }
                    }
                }
            }
            result = new AggregationResults(name, buckets);
        }
        return result;
    }
}
