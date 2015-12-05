package com.scaleset.search.es.agg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;

public class DateRangeAggregationConverter extends AbstractCombinedConverter {

    private DateMathParser dateMathParser = new DateMathParser(DateFieldMapper.Defaults.DATE_TIME_FORMATTER, DateFieldMapper.Defaults.TIME_UNIT);

    @Override
    public AggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

        DateRangeBuilder result = dateRange(getName(aggregation));

        addField(aggregation, result);
        addScript(aggregation, result);

        // TODO: Allow now to be passed in the aggregation
        long now = new Date().getTime();

        ArrayNode ranges = aggregation.get(ArrayNode.class, "ranges");
        if (ranges.isArray()) {
            for (JsonNode range : ranges) {
                Long from = toTimestamp(range.path("from"), now);
                Long to = toTimestamp(range.path("to"), now);
                String key = range.path("key").asText(null);
                if (from != null && to != null) {
                    result.addRange(key, from, to);
                } else if (from != null) {
                    result.addUnboundedFrom(key, from);
                } else if (to != null) {
                    result.addUnboundedTo(key, to);
                }
            }
        }

        addSubAggs(queryConverter, aggregation, result);
        return result;
    }

    Long toTimestamp(JsonNode node, long now) {
        if (node.isNumber()) {
            return node.asLong();
        } else if (node.isTextual()) {
            String text = node.asText();
            return dateMathParser.parse(text, now);
        } else {
            return null;
        }
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
                String key = bucket.getKey();
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
