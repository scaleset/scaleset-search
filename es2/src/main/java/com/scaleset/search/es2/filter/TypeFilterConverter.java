package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.typeQuery;


public class TypeFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {
        String value = filter.getString("value");
        return typeQuery(value);
    }

}
