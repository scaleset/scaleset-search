package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoPolygonFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {
        String field = filter.getString("field");
        Geometry geometry = filter.get(Geometry.class, "geometry");
        GeoPolygonQueryBuilder result = null;
        if (geometry instanceof Polygon) {
            result = QueryBuilders.geoPolygonQuery(field);
            for (Coordinate coord : ((Polygon) geometry).getExteriorRing().getCoordinates()) {
                result.addPoint(coord.y, coord.x);
            }
        }
        return result;
    }

}
