package com.scaleset.search;

import com.vividsolutions.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {

    private List<Aggregation> aggregations = new ArrayList<>();
    private Envelope bbox;
    private String geoField;
    private List<Filter> filters = new ArrayList<>();
    private int offset = 0;
    private Integer limit = 10;
    private String q = "";
    private List<Sort> sorts = new ArrayList<>();
    private Boolean echo;

    public QueryBuilder() {
    }

    public QueryBuilder(Query prototype) {
        if (prototype != null) {
            this.q = prototype.getQ();
            this.offset = prototype.getOffset();
            this.limit = prototype.getLimit();
            this.aggregation(prototype.getAggs());
            this.sort(prototype.getSorts());
        }
    }

    public QueryBuilder(String q) {
        q(q);
    }

    public static Double toDouble(String text, Double fallback) {
        Double result = fallback;
        if (text != null) {
            try {
                result = Double.valueOf(text);
            } catch (NumberFormatException e) {
            }
        }
        return fallback;
    }

    public static Integer toInteger(String text, Integer fallback) {
        Integer result = fallback;
        if (text != null) {
            try {
                result = Integer.valueOf(text);
            } catch (NumberFormatException e) {
            }
        }
        return result;
    }

    public QueryBuilder aggregation(Aggregation... aggregations) {
        if (aggregations != null) {
            for (Aggregation aggregation : aggregations) {
                this.aggregations.add(aggregation);
            }
        }
        return this;
    }

    public QueryBuilder aggregations(Iterable<Aggregation> aggs) {
        if (aggs != null) {
            for (Aggregation aggregation : aggs) {
                this.aggregations.add(aggregation);
            }
        }
        return this;
    }

    public QueryBuilder bbox(Envelope bbox) {
        this.bbox = bbox;
        return this;
    }

    public QueryBuilder bbox(String str) {
        if (str != null && !str.isEmpty()) {
            String[] parts = str.split(",");
            if (parts.length == 4) {
                double[] coords = new double[4];
                for (int i = 0; i < 4; ++i) {
                    coords[i] = toDouble(parts[i], 0.0);
                }
                Envelope bbox = new Envelope(coords[0], coords[2], coords[1], coords[3]);
                bbox(bbox);
            }
        }
        return this;
    }

    public Query build() {
        Query result = new Query(q, bbox, geoField, offset, limit, sorts, aggregations, filters);
        return result;
    }

    public QueryBuilder echo(boolean echo) {
        this.echo = echo;
        return this;
    }

    public QueryBuilder echo() {
        this.echo = true;
        return this;
    }

    public QueryBuilder filter(Filter... filters) {
        if (filters != null) {
            for (Filter filter : filters) {
                this.filters.add(filter);
            }
        }
        return this;
    }

    public QueryBuilder geoField(String geoField) {
        this.geoField = geoField;
        return this;
    }

    public QueryBuilder limit(int maxResults) {
        this.limit = maxResults;
        return this;
    }

    public QueryBuilder offset(int firstResult) {
        this.offset = firstResult;
        return this;
    }

    public QueryBuilder q(String q) {
        if (q != null) {
            q = q.trim();
        }
        if ("".equals(q)) {
            q = null;
        }
        this.q = q;
        return this;
    }

    public QueryBuilder sort(Iterable<Sort> sorts) {
        if (sorts != null) {
            for (Sort sort : sorts) {
                this.sorts.add(sort);
            }
        }
        return this;
    }

    public QueryBuilder sort(Sort... sorts) {
        if (sorts != null) {
            for (Sort sort : sorts) {
                this.sorts.add(sort);
            }
        }
        return this;
    }

    public QueryBuilder sort(String... fields) {
        if (sorts != null) {
            for (String field : fields) {
                this.sorts.add(new Sort(field, Sort.Direction.Ascending));
            }
        }
        return this;
    }

}
