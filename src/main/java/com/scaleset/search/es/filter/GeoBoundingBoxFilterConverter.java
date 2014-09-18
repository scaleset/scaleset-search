package com.scaleset.search.es.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Filter;
import com.scaleset.utils.Coerce;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

public class GeoBoundingBoxFilterConverter implements FilterConverter {

    private Coerce coerce = new Coerce(new ObjectMapper().registerModule(new GeoJsonModule()));

    public static final JtsSpatialContext SPATIAL_CONTEXT = JtsSpatialContext.GEO;
    public static final GeometryFactory FACTORY = SPATIAL_CONTEXT.getGeometryFactory();
    protected final boolean multiPolygonMayOverlap = false;

    @Override
    public FilterBuilder convert(Filter filter) {
        String name = filter.getName();
        Envelope bbox = filter.get(Envelope.class, "bbox");
        FilterBuilder result = FilterBuilders.geoBoundingBoxFilter(name)
                .bottomLeft(bbox.getMinY(), bbox.getMinX())
                .topRight(bbox.getMaxY(), bbox.getMaxX());
        return result;
    }

}
