package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class TermAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

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

        TermsBuilder result = terms(getName(aggregation));

        addField(aggregation, result);
        addScript(aggregation, result);

        Integer limit = aggregation.getInteger("limit");
        if (limit != null) {
            result.size(limit);
        }
        // todo - add ordering

        addSubAggs(queryConverter, aggregation, result);
        return result;
    }

    @Override
    public AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation aggregation, Aggregations aggs) {
        AggregationResults result = null;

        String name = aggregation.getName();
        if (aggs.get(name) instanceof Terms) {
            Terms terms = (Terms) aggs.get(name);
            List<Bucket> buckets = new ArrayList<>();
            for (Terms.Bucket bucket : terms.getBuckets()) {
                Bucket b = new Bucket(bucket.getKey(), bucket.getDocCount());
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
