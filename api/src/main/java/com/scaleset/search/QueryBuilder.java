package com.scaleset.search;

import com.scaleset.utils.Coerce;
import com.vividsolutions.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryBuilder {

    private Map<String, Aggregation> aggregations = new HashMap<>();
    private Envelope bbox;
    private String geoField;
    private Map<String, Filter> filters = new HashMap<>();
    private int offset = 0;
    private List<String> fields = new ArrayList<>();
    private Integer limit = 10;
    private String q = "";
    private List<Sort> sorts = new ArrayList<>();
    private Map<String, Object> headers = new HashMap<>();

    public QueryBuilder() {
    }

    public QueryBuilder(Query prototype) {
        if (prototype != null) {
            this.q = prototype.getQ();
            this.offset = prototype.getOffset();
            this.limit = prototype.getLimit();
            this.aggregations(prototype.getAggs());
            this.filters(prototype.getFilters());
            this.sort(prototype.getSorts());
            this.field(prototype.getFields());
            this.headers(prototype.getHeaders());
        }
    }

    public QueryBuilder(String q) {
        q(q);
    }

    public Aggregation agg(String name) {
        Aggregation result = new Aggregation(name);
        aggregation(result);
        return result;
    }

    public QueryBuilder aggregation(Aggregation... aggregations) {
        if (aggregations != null) {
            for (Aggregation aggregation : aggregations) {
                this.aggregations.put(aggregation.getName(), aggregation);
            }
        }
        return this;
    }

    protected QueryBuilder aggregations(Map<String, Aggregation> aggs) {
        for (String name : aggs.keySet()) {
            this.aggregations.put(name, aggs.get(name));
        }
        return this;
    }

    protected QueryBuilder filters(Map<String, Filter> filters) {
        for (String name : filters.keySet()) {
            this.filters.put(name, filters.get(name));
        }
        return this;
    }

    public QueryBuilder aggregations(Iterable<Aggregation> aggs) {
        if (aggs != null) {
            for (Aggregation aggregation : aggs) {
                this.aggregations.put(aggregation.getName(), aggregation);
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
                    coords[i] = Coerce.toDouble(parts[i], 0.0);
                }
                Envelope bbox = new Envelope(coords[0], coords[2], coords[1], coords[3]);
                bbox(bbox);
            }
        }
        return this;
    }

    public Query build() {
        Query result = new Query(q, bbox, geoField, fields, offset, limit, sorts, aggregations, filters, headers);
        return result;
    }

    public QueryBuilder echo(boolean echo) {
        headers.put("echo", echo);
        return this;
    }

    public QueryBuilder echo() {
        headers.put("echo", true);
        return this;
    }

    public QueryBuilder filter(Filter... filters) {
        if (filters != null) {
            for (Filter filter : filters) {
                this.filters.put(filter.getName(), filter);
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

    public QueryBuilder field(String... fields) {
        if (fields != null) {
            for (String field : fields) {
                this.fields.add(field);
            }
        }
        return this;
    }

    public QueryBuilder headers(Map<String, Object> headers) {
        this.headers.putAll(headers);
        return this;
    }

    public QueryBuilder header(String name, Object value) {
        headers.put(name, value);
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
