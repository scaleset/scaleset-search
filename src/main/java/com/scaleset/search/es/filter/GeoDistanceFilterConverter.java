package com.scaleset.search.es.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Filter;
import com.scaleset.utils.Coerce;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

public class GeoDistanceFilterConverter implements FilterConverter {

    private Coerce coerce = new Coerce(new ObjectMapper().registerModule(new GeoJsonModule()));

    @Override
    public FilterBuilder convert(Filter filter) {
        String field = filter.getString("field");
        String distance = filter.getString("distance");
        Geometry geometry = filter.get(Geometry.class, "geometry");
        Coordinate coord = geometry.getCentroid().getCoordinate();
        FilterBuilder result = FilterBuilders.geoDistanceFilter(field)
                .lon(coord.x).lat(coord.y)
                .distance(distance)
                .optimizeBbox("indexed");
        return result;
    }

}
