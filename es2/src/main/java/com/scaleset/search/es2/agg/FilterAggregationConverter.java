package com.scaleset.search.es2.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es2.QueryConverter;
import com.scaleset.search.es2.ResultsConverter;
import com.scaleset.utils.Coerce;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;

public class FilterAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AggregationBuilder convert(QueryConverter converter, Aggregation aggregation) {

        com.scaleset.search.Filter filter = Coerce.to(aggregation.get("filter"), com.scaleset.search.Filter.class);
        QueryBuilder fb = converter.converterFilter(filter);
        FilterAggregationBuilder result = filter(getName(aggregation)).filter(fb);
        addSubAggs(converter, aggregation, result);
        return result;
    }

    @Override
    public AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation aggregation, Aggregations aggs) {
        AggregationResults result = null;

        String name = aggregation.getName();
        if (aggs.get(name) instanceof Filter) {
            Filter filter = aggs.get(name);
            List<Bucket> buckets = new ArrayList<>();
            Bucket b = new Bucket(filter.getName(), filter.getDocCount());
            buckets.add(b);
            result = new AggregationResults(name, buckets);
        }
        return result;
    }

}
