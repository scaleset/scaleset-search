package com.scaleset.search.es2.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class RangeFilterConverter implements FilterConverter {

    @Override
    public QueryBuilder convert(Filter filter) {

        String field = filter.getString("field");
        if (field == null) {
            field = filter.getName();
        }

        RangeQueryBuilder result = rangeQuery(field);
        String gte = filter.getString("gte");
        String gt = filter.getString("gt");
        String lt = filter.getString("lt");
        String lte = filter.getString("lte");
        String time_zone = filter.getString("time_zone");

        if (gte != null) {
            result.gte(gte);
        }
        if (gt != null) {
            result.gt(gt);
        }
        if (lt != null) {
            result.lt(lt);
        }
        if (lte != null) {
            result.lte(lte);
        }
        if (time_zone != null) {
            result.timeZone(time_zone);
        }
        return result;
    }

}