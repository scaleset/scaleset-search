package com.scaleset.search.es;

import com.scaleset.geo.Feature;

public class FeatureSearchMapping extends AbstractSearchMapping<Feature, String> {

    public FeatureSearchMapping() {
        super(Feature.class, "features", "feature");
    }

    @Override
    public String id(Feature feature) throws Exception {
        return feature.getId();
    }

    @Override
    public String idForKey(String key) throws Exception {
        return key;
    }
}
