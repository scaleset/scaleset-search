package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;

public class GeoHashGridStatsAggregationConverter extends AbstractCombinedConverter {

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
        result.subAggregation(stats("lat_stats").field(field + ".lat"));
        result.subAggregation(stats("lon_stats").field(field + ".lon"));

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
            GeoHashGrid grid = (GeoHashGrid) (aggs.get(name));
            List<Bucket> buckets = new ArrayList<>();
            for (GeoHashGrid.Bucket bucket : grid.getBuckets()) {
                if (bucket.getDocCount() > 0) {
                    Bucket b = new Bucket(bucket.getKey(), bucket.getDocCount());
                    Stats latStats = bucket.getAggregations().get("lat_stats");
                    Stats lonStats = bucket.getAggregations().get("lon_stats");
                    double minx = lonStats.getMin();
                    double maxx = lonStats.getMax();
                    double miny = latStats.getMin();
                    double maxy = latStats.getMax();
                    double cx = lonStats.getAvg();
                    double cy = latStats.getAvg();
                    Envelope bbox = new Envelope(minx, maxx, miny, maxy);
                    b.put("bbox", bbox);
                    b.put("centroid", new Coordinate(cx, cy));
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
