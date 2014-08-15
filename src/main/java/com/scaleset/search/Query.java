package com.scaleset.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.vividsolutions.jts.geom.Envelope;

import java.util.List;

@JsonInclude(Include.NON_EMPTY)
public class Query {

    private Aggregation[] aggs;

    private Envelope bbox;

    private String geoField;

    private int limit;

    private int offset;

    private String q;

    private Sort[] sorts = new Sort[0];

    /* For Jackson */
    public Query() {
    }

    public Query(String q, Envelope bbox, String geoField, int offset, int limit, List<Sort> sorts, List<Aggregation> aggs) {
        this.q = q;
        this.offset = offset;
        this.limit = limit;
        this.bbox = bbox;
        this.geoField = geoField;
        this.sorts = sorts.toArray(new Sort[sorts.size()]);
        this.aggs = aggs.toArray(new Aggregation[aggs.size()]);
    }

    public Aggregation[] getAggs() {
        return aggs;
    }

    public Envelope getBbox() {
        return bbox;
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
