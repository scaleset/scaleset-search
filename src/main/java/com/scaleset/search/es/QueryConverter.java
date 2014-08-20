package com.scaleset.search.es;

import com.scaleset.search.Aggregation;
import com.scaleset.search.Query;
import com.scaleset.search.Sort;
import com.vividsolutions.jts.geom.Envelope;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder.Operator;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class QueryConverter {

    //private SearchRequestBuilder builder;
    private Client client;
    private String index;
    private String type;
    private Query query;
    //private BoolFilterBuilder boolFilter = boolFilter();
    private Map<String, AggregationConverter> converters = new HashMap<>();

    public QueryConverter(Client client, Query query, String index) {
        this.client = client;
        this.query = query;
        this.index = index;
        registerDefaultConverters();
    }

    public QueryConverter(Client client, Query query, String index, String type) {
        this.client = client;
        this.query = query;
        this.index = index;
        this.type = type;
        registerDefaultConverters();
    }

    protected void addAggregations(SearchRequestBuilder builder) {
        Aggregation[] aggs = query.getAggs();
        if (aggs != null) {
            for (Aggregation agg : aggs) {
                String type = agg.getType();
                AggregationConverter converter = converters.get(type);
                if (converter != null) {
                    AbstractAggregationBuilder aggregationBuilder = converter.convert(agg);
                    builder.addAggregation(aggregationBuilder);
                }
            }
        }
    }

    protected void addPaging(SearchRequestBuilder builder) {
        int limit = query.getLimit();
        builder.setSize(limit);
    }

    protected void addQ(SearchRequestBuilder builder, BoolFilterBuilder boolFilter) {
        if (type != null && !type.isEmpty()) {
            builder.setTypes(type);
        }
        if (query.getQ() != null && !query.getQ().isEmpty()) {
            boolFilter.must(queryFilter(queryString(query.getQ()).defaultOperator(Operator.AND)));
        }
    }

    protected void addQ(DeleteByQueryRequestBuilder builder, BoolFilterBuilder boolFilter) {
        if (type != null && !type.isEmpty()) {
            builder.setTypes(type);
        }
        if (query.getQ() != null && !query.getQ().isEmpty()) {
            boolFilter.must(queryFilter(queryString(query.getQ()).defaultOperator(Operator.AND)));
        }
    }

    protected void addBbox(SearchRequestBuilder builder, BoolFilterBuilder boolFilter) {
        Envelope bbox = query.getBbox();
        String geoField = query.getGeoField();
        if ((geoField != null && !geoField.isEmpty()) && (bbox != null)) {
            boolFilter.must(geoBoundingBoxFilter(geoField)
                    .bottomLeft(bbox.getMinY(), bbox.getMinX())
                    .topRight(bbox.getMaxY(), bbox.getMaxX()));
        }
    }

    protected void addBbox(DeleteByQueryRequestBuilder builder, BoolFilterBuilder boolFilter) {
        Envelope bbox = query.getBbox();
        String geoField = query.getGeoField();
        if ((geoField != null && !geoField.isEmpty()) && (bbox != null)) {
            boolFilter.must(geoBoundingBoxFilter(geoField)
                    .bottomLeft(bbox.getMinY(), bbox.getMinX())
                    .topRight(bbox.getMaxY(), bbox.getMaxX()));
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

    public SearchRequestBuilder searchRequest() {
        SearchRequestBuilder builder = client.prepareSearch(index);
        builder.setSearchType(SearchType.DEFAULT);
        BoolFilterBuilder boolFilter = boolFilter();
        addPaging(builder);
        addQ(builder, boolFilter);
        addBbox(builder, boolFilter);
        setFilter(builder, boolFilter);
        addAggregations(builder);
        addSorts(builder);
        return builder;
    }

    public DeleteByQueryRequestBuilder deleteRequest() {
        DeleteByQueryRequestBuilder builder = client.prepareDeleteByQuery(index);
        BoolFilterBuilder boolFilter = boolFilter();
        addQ(builder, boolFilter);
        addBbox(builder, boolFilter);
        setFilter(builder, boolFilter);
        return builder;
    }

    public QueryConverter register(String type, AggregationConverter converter) {
        converters.put(type, converter);
        return this;
    }

    public org.elasticsearch.index.query.QueryBuilder query() {
        org.elasticsearch.index.query.QueryBuilder result = null;
        String q = query.getQ();
        if (q != null && !q.isEmpty()) {
            result = QueryBuilders.queryString(q).defaultOperator(QueryStringQueryBuilder.Operator.OR);
        }
        return result;
    }

    protected void registerDefaultConverters() {
        register("term", new TermAggregatinConverter());
    }

}
