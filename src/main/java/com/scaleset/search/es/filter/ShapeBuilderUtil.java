package com.scaleset.search.es.filter;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.overlay.PointBuilder;
import org.elasticsearch.common.geo.builders.BasePolygonBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

public class ShapeBuilderUtil {

    public static ShapeBuilder toShapeBuilder(Geometry geometry) {
        ShapeBuilder result = null;
        if (geometry instanceof Point) {
            result = toShapeBuilder((Point) geometry);
        }
        if (geometry instanceof Polygon) {
            result = toShapeBuilder((Polygon) geometry);
        }
        return result;
    }

    public static ShapeBuilder toShapeBuilder(Polygon polygon) {
        PolygonBuilder result = ShapeBuilder.newPolygon();
        LineString exterior = polygon.getExteriorRing();
        for (Coordinate coord : exterior.getCoordinates()) {
            result.point(coord);
        }
        int numInteriorRing = polygon.getNumInteriorRing();
        for (int i = 0; i < numInteriorRing; ++i) {
            LineString interior = polygon.getInteriorRingN(i);
            BasePolygonBuilder.Ring<PolygonBuilder> hole = result.hole();
            for (Coordinate coord : interior.getCoordinates()) {
                hole.point(coord);
            }
        }
        return result;
    }

    public static ShapeBuilder toShapeBuilder(Point point) {
        return ShapeBuilder.newPoint(point.getCoordinate());
    }

}
