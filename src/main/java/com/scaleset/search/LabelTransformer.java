package com.scaleset.search;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LabelTransformer<T> {

    private LabelProvider defaultLabelProvider;
    private Map<String, LabelProvider> labelProviders = new HashMap<>();

    public LabelTransformer(LabelProvider defaultLabelProvider) {
        this.defaultLabelProvider = defaultLabelProvider;
    }

    public LabelTransformer<T> provider(String name, LabelProvider provider) {
        labelProviders.put(name, provider);
        return this;
    }

    public Results<T> transform(Results<T> input) {
        List<AggregationResults> updatedAggs = new ArrayList<>();
        List<AggregationResults> aggs = input.getAggs();
        for (AggregationResults fc : aggs) {
            List<Bucket> counts = fc.getBuckets();
            List<Bucket> updatedCounts = new ArrayList<>();
            for (Bucket count : counts) {
                Object value = count.getKey();
                long number = count.getCount();
                String name = fc.getName();
                LabelProvider provider = defaultLabelProvider;
                if (labelProviders.containsKey(name)) {
                    provider = labelProviders.get(name);
                }
                String label = provider.label(fc.getName(), count);
                updatedCounts.add(new Bucket(value, number, label));
            }
            updatedAggs.add(new AggregationResults(fc.getName(), updatedCounts));
        }
        Results<T> results = new Results<T>(input.getQuery(), updatedAggs, input.getItems(), input.getTotalItems());
        return results;
    }
}
