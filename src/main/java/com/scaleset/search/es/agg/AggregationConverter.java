package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface AggregationConverter {

    AbstractAggregationBuilder convert(Aggregation aggregation);

}
