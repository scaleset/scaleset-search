package com.scaleset.search.es.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import static org.elasticsearch.index.query.FilterBuilders.queryFilter;
import static org.elasticsearch.index.query.QueryBuilders.queryString;

public class QueryFilterConverter implements FilterConverter {

    @Override
    public FilterBuilder convert(Filter filter) {

        // TODO: add options for operator and caching;

        QueryStringQueryBuilder.Operator operator = QueryStringQueryBuilder.Operator.AND;
        boolean cache = filter.getBoolean("cache", false);

        String q = filter.getString("query");
        FilterBuilder result = queryFilter(queryString(q).defaultOperator(operator)).cache(cache);
        return result;
    }

}