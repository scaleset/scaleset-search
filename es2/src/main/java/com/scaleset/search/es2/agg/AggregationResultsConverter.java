package com.scaleset.search.es2.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.es2.ResultsConverter;
import org.elasticsearch.search.aggregations.Aggregations;

public interface AggregationResultsConverter {

    AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation aggregation, Aggregations aggs);

}
