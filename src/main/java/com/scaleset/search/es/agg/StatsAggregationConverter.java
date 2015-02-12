package com.scaleset.search.es.agg;

import com.scaleset.search.Aggregation;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.es.QueryConverter;
import com.scaleset.search.es.ResultsConverter;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;

public class StatsAggregationConverter extends AbstractCombinedConverter {

    @Override
    public AbstractAggregationBuilder convert(QueryConverter queryConverter, Aggregation aggregation) {

        StatsBuilder result = AggregationBuilders.stats(getName(aggregation));
        String field = aggregation.getString("field");

        if (field != null) {
            result.field(field);
        }
        return result;
    }

    @Override
    public AggregationResults convertResult(ResultsConverter resultsConverter, Aggregation
            aggregation, Aggregations aggs) {
        String name = aggregation.getName();
        AggregationResults result = null;
        if (aggs.get(name) instanceof Stats) {
            Stats stats = (Stats) (aggs.get(name));
            long count = stats.getCount();
            double sum = stats.getSum();
            double min = stats.getMin();
            double max = stats.getMax();
            double mean = stats.getAvg();
            com.scaleset.search.Stats resultStats = new com.scaleset.search.Stats(count, sum, min, max, mean);
            result = new AggregationResults(name, resultStats);
        }
        return result;
    }
}
