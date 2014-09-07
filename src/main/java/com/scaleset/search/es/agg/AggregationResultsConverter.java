package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import org.elasticsearch.action.search.SearchResponse;

public interface AggregationResultsConverter {

    AggregationResults convertResult(Aggregation aggregation, SearchResponse response);

}
