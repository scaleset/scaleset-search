package com.scaleset.search.es2.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.es2.QueryConverter;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface AggregationConverter {

    AbstractAggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation);

}
