package com.scaleset.search.es.agg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.range;

public class RangeAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

        RangeBuilder result = range(getName(aggregation));

        addField(aggregation, result);
        addScript(aggregation, result);

        ArrayNode ranges = aggregation.get(ArrayNode.class, "ranges");
        if (ranges.isArray()) {
            for (JsonNode range : ranges) {
                JsonNode from = range.path("from");
                JsonNode to = range.path("to");
                if (from.isNumber() && to.isNumber()) {
                    result.addRange(from.asDouble(), to.asDouble());
                } else if (from.isNumber()) {
                    result.addUnboundedFrom(from.asDouble());
                } else if (to.isNumber()) {
                    result.addUnboundedTo(to.asDouble());
                }
            }
        }

        addSubAggs(queryConverter, aggregation, result);
        return result;
    }

    @Override
    public AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation aggregation, Aggregations aggs) {
        AggregationResults result = null;

        String name = aggregation.getName();
        if (aggs.get(name) instanceof Range) {
            Range ranges = (Range) aggs.get(name);
            List<Bucket> buckets = new ArrayList<>();
            for (Range.Bucket bucket : ranges.getBuckets()) {
                String label = bucket.getFrom() + " TO " + bucket.getTo();
                String key = "[" + label + "}";
                Bucket b = new Bucket(key, bucket.getDocCount(), label);
                buckets.add(b);
                for (Aggregation subAgg : aggregation.getAggs().values()) {
                    AggregationResults subResults = resultsConverter.convertResults(subAgg, bucket.getAggregations());
                    if (subResults != null) {
                        b.getAggs().put(subAgg.getName(), subResults);
                    }
                }
            }
            result = new AggregationResults(name, buckets);
        }
        return result;
    }

}
