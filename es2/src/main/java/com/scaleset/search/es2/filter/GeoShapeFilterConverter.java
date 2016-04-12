package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import com.vividsolutions.jts.geom.Geometry;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoShapeFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {
        String field = filter.getString("field");
        Geometry geometry = filter.get(Geometry.class, "geometry");
        String relationName = filter.getString("relation");
        ShapeBuilder shapeBuilder = ShapeBuilderUtil.toShapeBuilder(geometry);
        ShapeRelation shapeRelation = ShapeRelation.getRelationByName(relationName);
        return QueryBuilders.geoShapeQuery(field, shapeBuilder, shapeRelation);
    }

}
