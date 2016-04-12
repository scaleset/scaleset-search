package com.scaleset.search.es2;

import com.scaleset.geo.Feature;
import com.scaleset.search.es2.AbstractSearchMapping;

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
