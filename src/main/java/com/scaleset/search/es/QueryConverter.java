package com.scaleset.search.es;

import com.scaleset.search.Aggregation;
import com.scaleset.search.Filter;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface QueryConverter {

    AbstractAggregationBuilder converterAggregation(Aggregation agg);

    FilterBuilder converterFilter(Filter filter);

    DeleteByQueryRequestBuilder deleteRequest();

    org.elasticsearch.index.query.QueryBuilder query();

    SearchRequestBuilder searchRequest();
}
