package com.scaleset.search.es.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Filter;
import com.scaleset.utils.Coerce;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import static org.elasticsearch.index.query.FilterBuilders.queryFilter;
import static org.elasticsearch.index.query.FilterBuilders.typeFilter;
import static org.elasticsearch.index.query.QueryBuilders.queryString;

public class TypeFilterConverter implements FilterConverter {

    @Override
    public FilterBuilder convert(Filter filter) {
        String value = filter.getString("value");
        FilterBuilder result = typeFilter(value);
        return result;
    }

}
