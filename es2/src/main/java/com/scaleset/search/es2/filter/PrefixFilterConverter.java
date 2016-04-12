package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;

public class PrefixFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {

        String q = filter.getString("query");
        String field = filter.getString("field");
        return prefixQuery(field, q);
    }
}