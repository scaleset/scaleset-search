package com.scaleset.search.es2.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es2.QueryConverter;
import com.scaleset.search.es2.ResultsConverter;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;

import java.util.ArrayList;
import java.util.List;

public class GeoHashGridAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

        GeoHashGridBuilder result = AggregationBuilders.geohashGrid(getName(aggregation));
        String field = aggregation.getString("field");
        Integer precision = aggregation.getInteger("precision");
        Integer size = aggregation.getInteger("size");
        if (field != null) {
            result.field(field);
        }
        if (precision != null) {
            result.precision(precision);
        }
        if (size != null) {
            result.size(size);
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
        if (aggs.get(name) instanceof GeoHashGrid) {
            GeoHashGrid grid = aggs.get(name);
            List<Bucket> buckets = new ArrayList<>();
            grid.getBuckets().stream().filter(bucket -> bucket.getDocCount() > 0).forEach(bucket -> {
                Bucket b = new Bucket(bucket.getKey(), bucket.getDocCount());
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
