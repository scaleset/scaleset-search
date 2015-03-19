package com.scaleset.search.es.filter;

import com.scaleset.search.Filter;

import org.elasticsearch.index.query.FilterBuilder;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;

public class TermFilterConverter implements FilterConverter {

    @Override
    public FilterBuilder convert(Filter filter) {
        boolean cache = filter.getBoolean("cache", false);

        String q = filter.getString("query");
        String field = filter.getString("field");
        FilterBuilder result = termFilter(field,q).cache(cache);
        return result;
    }

}