package com.scaleset.search.es.filter;

import com.scaleset.search.Filter;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;

import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;

public class RangeFilterConverter implements FilterConverter {

    @Override
    public FilterBuilder convert(Filter filter) {

        boolean cache = filter.getBoolean("cache", false);
        String field = filter.getString("field");
        if (field == null) {
            field = filter.getName();
        }

        RangeFilterBuilder result = rangeFilter(field).cache(cache);
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