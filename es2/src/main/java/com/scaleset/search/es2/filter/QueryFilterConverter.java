package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

public class QueryFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {
        QueryStringQueryBuilder.Operator operator = QueryStringQueryBuilder.Operator.AND;
        String q = filter.getString("query");
        return queryStringQuery(q).defaultOperator(operator);
    }

}