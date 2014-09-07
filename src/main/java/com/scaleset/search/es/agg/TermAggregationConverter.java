package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class TermAggregationConverter implements AggregationConverter, AggregationResultsConverter {

    @Override
    public AbstractAggregationBuilder convert(Aggregation aggregation) {

        /*
        Terms.Order order = Terms.Order.count(false);
        Sort sort = aggregation.getSort();
        if (sort != null) {
            boolean asc = Sort.Direction.Ascending.equals(sort.getDirection());
            if (Sort.Type.Count.equals(sort.getType())) {
                order = Terms.Order.count(asc);
            } else if (Sort.Type.Lexical.equals(sort.getType())) {
                order = Terms.Order.term(asc);
            }
        }
        */
        String name = aggregation.getName();
        String field = aggregation.getString("field");
        String script = aggregation.getString("script");
        Integer limit = aggregation.getInteger("limit");

        if (name == null) {
            name = field;
        }

        TermsBuilder result = terms(name);
        result.field(field);
        if (script != null) {
            result.script(script);
        }
        if (limit != null) {
            result.size(limit);
        }
        // todo - process subaggs;
        // todo - add ordering
        return result;
    }

    @Override
    public AggregationResults convertResult(Aggregation aggregation, SearchResponse response) {
        String name = aggregation.getName();
        org.elasticsearch.search.aggregations.Aggregation agg = response.getAggregations().get(name);
        AggregationResults result = null;
        if (agg instanceof Terms) {
            Terms terms = (Terms) agg;
            List<Bucket> buckets = new ArrayList<>();
            for (Terms.Bucket bucket : terms.getBuckets()) {
                buckets.add(new Bucket(bucket.getKey(), bucket.getDocCount()));
            }
            result = new AggregationResults(agg.getName(), buckets);
        }
        return result;
    }

}
