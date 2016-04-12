package com.scaleset.search.es2;

import com.scaleset.search.Aggregation;
import com.scaleset.search.Filter;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface QueryConverter {

    AbstractAggregationBuilder converterAggregation(Aggregation agg);

    QueryBuilder converterFilter(Filter filter);

    org.elasticsearch.index.query.QueryBuilder query();

    SearchRequestBuilder searchRequest();
}
