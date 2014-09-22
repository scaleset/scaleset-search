package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.es.QueryConverter;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface AggregationConverter {

    AbstractAggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation);

}
