package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.es.QueryConverter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;

import java.util.Map;

public abstract class AbstractCombinedConverter implements AggregationConverter, AggregationResultsConverter {

    protected String getName(Aggregation agg) {
        String result = agg.getName();
        String field = agg.getString("field");

        if (result == null) {
            result = field;
        }
        return result;
    }

    protected void addField(Aggregation agg, ValuesSourceAggregationBuilder<?> builder) {
        // source
        String field = agg.getString("field");
        if (field != null) {
            builder.field(field);
        }
    }

    protected void addScript(Aggregation agg, ValuesSourceAggregationBuilder<?> builder) {
        String script = agg.getString("script");

        if (script != null) {
            builder.script(script);
        }
    }

    // add sub-aggs when building search request
    protected void addSubAggs(QueryConverter queryConverter, Aggregation agg, AggregationBuilder builder) {
        Map<String, Aggregation> aggs = agg.getAggs();
        for (String key : aggs.keySet()) {
            Aggregation subAgg = aggs.get(key);
            AbstractAggregationBuilder subBuilder = queryConverter.converterAggregation(subAgg);
            if (subBuilder != null) {
                builder.subAggregation(subBuilder);
            }
        }
    }
}
