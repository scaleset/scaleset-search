package com.scaleset.search.es.filter;

import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;

import org.elasticsearch.index.query.FilterBuilder;

import com.scaleset.search.Filter;

public class PrefixFilterConverter implements FilterConverter {

    @Override
    public FilterBuilder convert(Filter filter) {
        boolean cache = filter.getBoolean("cache", false);

        String q = filter.getString("query");
        String field = filter.getString("field");
        FilterBuilder result = prefixFilter(field,q).cache(cache);
        return result;
    }
}