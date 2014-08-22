package com.scaleset.search.es;

import com.scaleset.search.Aggregation;
import com.scaleset.search.Sort;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class TermAggregatinConverter implements AggregationConverter {

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

}
