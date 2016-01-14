package com.scaleset.search.es;

import com.scaleset.search.Aggregation;
import com.scaleset.search.Filter;
import com.scaleset.search.Query;
import com.scaleset.search.Sort;
import com.scaleset.search.es.agg.*;
import com.scaleset.search.es.filter.*;
import com.scaleset.utils.Coerce;
import com.vividsolutions.jts.geom.Envelope;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder.Operator;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class DefaultQueryConverter implements QueryConverter {

    private Client client;
    private String[] indices = new String[0];
    private String[] types = new String[0];
    private Query query;
    private Map<String, AggregationConverter> converters = new HashMap<>();
    private Map<String, FilterConverter> filterConverters = new HashMap<>();
    private Map<String, AggregationResultsConverter> aggregationResultsConverters = new HashMap<>();

    public DefaultQueryConverter(Client client, Query query, String index) {
        this(client, query, index, null);
    }

    public DefaultQueryConverter(Client client, Query query, String[] indices) {
        this(client, query, indices, null);
    }

    public DefaultQueryConverter(Client client, Query query, String index, String[] types) {
        this(client, query, new String[]{index}, types);
    }

    public DefaultQueryConverter(Client client, Query query, String[] indices, String[] types) {
        this.client = client;
        this.query = query;
        this.indices = indices;
        this.types = types;
        registerDefaultConverters();
        registerDefaultFilterConverters();
    }

    protected void addAggregations(SearchRequestBuilder builder) {
        Map<String, Aggregation> aggs = query.getAggs();
        if (aggs != null) {
            for (String name : aggs.keySet()) {
                Aggregation agg = aggs.get(name);
                AbstractAggregationBuilder aggregationBuilder = converterAggregation(agg);
                if (aggregationBuilder != null) {
                    builder.addAggregation(aggregationBuilder);
                }
            }
        }
    }

    protected void addFilters(BoolFilterBuilder boolFilter) {
        Map<String, Filter> filters = query.getFilters();
        if (filters != null) {
            for (Filter filter : filters.values()) {
                FilterBuilder filterBuilder = converterFilter(filter);
                if (filterBuilder != null) {
                    switch (filter.getClause()) {
                        case SHOULD:
                            boolFilter.should(filterBuilder);
                            break;
                        case MUST_NOT:
                            boolFilter.mustNot(filterBuilder);
                            break;
                        case MUST:
                        default:
                            boolFilter.must(filterBuilder);
                    }
                }
            }
        }
    }

    @Override
    public AbstractAggregationBuilder converterAggregation(Aggregation agg) {
        AbstractAggregationBuilder result = null;
        if (agg != null) {
            String type = agg.getType();
            AggregationConverter converter = converters.get(type);
            if (converter != null) {
                result = converter.convert(this, agg);
            }
        }
        return result;
    }

    @Override
    public FilterBuilder converterFilter(Filter filter) {
        FilterBuilder result = null;
        if (filter != null) {
            String type = filter.getType();
            FilterConverter converter = filterConverters.get(type);
            if (converter != null) {
                result = converter.convert(filter);
            }
        }
        return result;
    }

    protected void addPaging(SearchRequestBuilder builder) {
        int limit = query.getLimit();
        int offset = query.getOffset();
        builder.setSize(limit);
        builder.setFrom(offset);
        String keepAlive = Coerce.toString(query.getHeaders().get("keepAlive"), null);
        if (keepAlive != null) {
            builder.setScroll(keepAlive);
        }
    }

    protected void addQ(SearchRequestBuilder builder, BoolFilterBuilder boolFilter) {
        if (types.length > 0) {
            builder.setTypes(types);
        }
        if (query.getQ() != null && !query.getQ().isEmpty()) {
            boolFilter.must(queryFilter(queryString(query.getQ()).defaultOperator(Operator.AND)));
        }
    }

    protected void addQ(DeleteByQueryRequestBuilder builder, BoolFilterBuilder boolFilter) {
        if (types.length > 0) {
            builder.setTypes(types);
        }
        if (query.getQ() != null && !query.getQ().isEmpty()) {
            boolFilter.must(queryFilter(queryString(query.getQ()).defaultOperator(Operator.AND)));
        }
    }

    protected void addBbox(BoolFilterBuilder boolFilter) {
        Envelope bbox = query.getBbox();
        String geoField = query.getGeoField();
        if ((geoField != null && !geoField.isEmpty()) && (bbox != null)) {
            boolFilter.must(geoBoundingBoxFilter(geoField).bottomLeft(bbox.getMinY(), bbox.getMinX()).topRight(bbox.getMaxY(), bbox.getMaxX()));
        }
    }

    protected void setFilter(SearchRequestBuilder builder, BoolFilterBuilder boolFilter) {
        if (boolFilter.hasClauses()) {
            builder.setQuery(filteredQuery(matchAllQuery(), boolFilter));
        }
    }

    protected void setFilter(DeleteByQueryRequestBuilder builder, BoolFilterBuilder boolFilter) {
        if (boolFilter.hasClauses()) {
            builder.setQuery(filteredQuery(matchAllQuery(), boolFilter));
        }
    }

    protected void addSorts(SearchRequestBuilder builder) {
        for (Sort sort : query.getSorts()) {
            String field = sort.getField();
            SortOrder order = Sort.Direction.Ascending.equals(sort.getDirection()) ? SortOrder.ASC : SortOrder.DESC;
            builder.addSort(SortBuilders.fieldSort(field).order(order));
        }
    }

    protected void addFields(SearchRequestBuilder builder) {
        if (query.getFields().length > 0) {
            builder.addFields(query.getFields());
        }
    }

    @Override
    public SearchRequestBuilder searchRequest() {
        SearchRequestBuilder builder = client.prepareSearch(indices);
        builder.setSearchType(SearchType.DEFAULT);
        BoolFilterBuilder boolFilter = boolFilter();
        addPaging(builder);
        addQ(builder, boolFilter);
        addFilters(boolFilter);
        addBbox(boolFilter);
        setFilter(builder, boolFilter);
        addAggregations(builder);
        addSorts(builder);
        addFields(builder);
        return builder;
    }

    @Override
    public DeleteByQueryRequestBuilder deleteRequest() {
        DeleteByQueryRequestBuilder builder = client.prepareDeleteByQuery(indices);
        BoolFilterBuilder boolFilter = boolFilter();
        addQ(builder, boolFilter);
        addFilters(boolFilter);
        addBbox(boolFilter);
        setFilter(builder, boolFilter);
        return builder;
    }

    public DefaultQueryConverter register(String type, AggregationConverter converter) {
        converters.put(type, converter);
        return this;
    }

    public DefaultQueryConverter register(String type, FilterConverter converter) {
        filterConverters.put(type, converter);
        return this;
    }

    @Override
    public org.elasticsearch.index.query.QueryBuilder query() {
        org.elasticsearch.index.query.QueryBuilder result = null;
        String q = query.getQ();
        if (q != null && !q.isEmpty()) {
            result = queryString(q).defaultOperator(QueryStringQueryBuilder.Operator.OR);
        }
        return result;
    }

    protected void registerDefaultConverters() {
        register("terms", new TermAggregationConverter());
        register("geohash_grid_stats", new GeoHashGridStatsAggregationConverter());
        register("geohash_grid", new GeoHashGridAggregationConverter());
        register("filter", new FilterAggregationConverter());
        register("range", new RangeAggregationConverter());
        register("date_range", new DateRangeAggregationConverter());
        register("stats", new StatsAggregationConverter());
        register("date_histogram", new DateHistogramAggregationConverter());
    }

    protected void registerDefaultFilterConverters() {
        register("geo_bounding_box", new GeoBoundingBoxFilterConverter());
        register("geo_distance", new GeoDistanceFilterConverter());
        register("geo_shape", new GeoShapeFilterConverter());
        register("geo_polygon", new GeoPolygonFilterConverter());
        register("query", new QueryFilterConverter());
        register("type", new TypeFilterConverter());
        register("prefix", new PrefixFilterConverter());
        register("term", new TermFilterConverter());
        register("range", new RangeFilterConverter());
    }

}
