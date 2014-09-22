package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.es.ResultsConverter;
import org.elasticsearch.search.aggregations.Aggregations;

public interface AggregationResultsConverter {

    AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation aggregation, Aggregations aggs);

}
