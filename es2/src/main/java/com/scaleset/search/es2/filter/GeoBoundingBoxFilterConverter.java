package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import com.vividsolutions.jts.geom.Envelope;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoBoundingBoxFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {
        String name = filter.getName();
        Envelope bbox = filter.get(Envelope.class, "bbox");
        return QueryBuilders.geoBoundingBoxQuery(name)
                .bottomLeft(bbox.getMinY(), bbox.getMinX())
                .topRight(bbox.getMaxY(), bbox.getMaxX());
    }

}
