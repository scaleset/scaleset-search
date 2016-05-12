package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoDistanceFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {
        String field = filter.getString("field");
        String distance = filter.getString("distance");
        String optimizeBbox = filter.getString("optimizeBbox");
        Geometry geometry = filter.get(Geometry.class, "geometry");
        Coordinate coord = geometry.getCentroid().getCoordinate();
        GeoDistanceQueryBuilder result = QueryBuilders.geoDistanceQuery(field)
                .lon(coord.x).lat(coord.y)
                .distance(distance);
        if (optimizeBbox != null) {
            result.optimizeBbox("indexed");
        }
        return result;
    }

}
