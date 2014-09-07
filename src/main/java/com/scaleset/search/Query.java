package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.vividsolutions.jts.geom.Envelope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(Include.NON_EMPTY)
public class Query {

    private HashMap<String, Aggregation> aggs = new HashMap<>();

    private Envelope bbox;

    private Map<String, Filter> filters = new HashMap<>();

    private String geoField;

    private int limit;

    private int offset = 0;

    private String q;

    private Sort[] sorts = new Sort[0];

    /* For Jackson */
    public Query() {
    }

    public Query(String q, Envelope bbox, String geoField, int offset, int limit, List<Sort> sorts, Map<String, Aggregation> aggs, Map<String, Filter> filters) {
        this.q = q;
        this.offset = offset;
        this.limit = limit;
        this.bbox = bbox;
        this.geoField = geoField;
        this.sorts = sorts.toArray(new Sort[sorts.size()]);
        if (aggs != null) {
            this.aggs.putAll(aggs);
        }
        if (filters != null) {
            this.filters.putAll(filters);
        }
    }

    public Map<String, Aggregation> getAggs() {
        return aggs;
    }

    public Envelope getBbox() {
        return bbox;
    }

    public Map<String, Filter> getFilters() {
        return filters;
    }

    public String getGeoField() {
        return geoField;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public String getQ() {
        return q;
    }

    public Sort[] getSorts() {
        return sorts;
    }

}
