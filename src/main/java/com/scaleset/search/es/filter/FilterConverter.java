package com.scaleset.search.es.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.FilterBuilder;

public interface FilterConverter {

    FilterBuilder convert(Filter filter);

}
