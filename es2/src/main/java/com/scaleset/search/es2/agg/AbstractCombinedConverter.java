package com.scaleset.search.es2.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.es2.QueryConverter;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
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

    void addField(Aggregation agg, ValuesSourceAggregationBuilder<?> builder) {
        // source
        String field = agg.getString("field");
        if (field != null) {
            builder.field(field);
        }
    }

    void addScript(Aggregation agg, ValuesSourceAggregationBuilder<?> builder) {
        String script = agg.getString("script");

        if (script != null) {
            builder.script(new Script(script));
        }
    }

    // add sub-aggs when building search request
    void addSubAggs(QueryConverter queryConverter, Aggregation agg, AggregationBuilder builder) {
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
