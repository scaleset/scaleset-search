package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Bucket;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import com.scaleset.utils.Coerce;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;

public class FilterAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

        com.scaleset.search.Filter filter = Coerce.to(aggregation.get("filter"), com.scaleset.search.Filter.class);
        FilterBuilder fb = queryConverter.converterFilter(filter);
        FilterAggregationBuilder result = filter(getName(aggregation)).filter(fb);
        addSubAggs(queryConverter, aggregation, result);
        return result;
    }

    @Override
    public AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation aggregation, Aggregations aggs) {
        AggregationResults result = null;

        String name = aggregation.getName();
        if (aggs.get(name) instanceof Filter) {
            Filter filter = (Filter) aggs.get(name);
            List<Bucket> buckets = new ArrayList<>();
            Bucket b = new Bucket(filter.getName(), filter.getDocCount());
            buckets.add(b);
            result = new AggregationResults(name, buckets);
        }
        return result;
    }

}
