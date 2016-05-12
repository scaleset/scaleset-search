package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.QueryBuilder;

public interface FilterConverter {

    QueryBuilder convert(Filter filter);

}
