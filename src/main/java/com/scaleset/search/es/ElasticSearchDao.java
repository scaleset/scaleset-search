package com.scaleset.search.es;

import com.scaleset.search.Query;
import com.scaleset.search.QueryBuilder;
import com.scaleset.search.Results;
import com.scaleset.search.GenericSearchDao;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchDao<T, K> implements GenericSearchDao<T, K> {

    protected Client client;
    protected SearchMapping<T, K> mapping;
    private Logger log = LoggerFactory.getLogger(getClass());

    protected ElasticSearchDao(Client client, SearchMapping<T, K> mapping) {
        this.client = client;
        this.mapping = mapping;
    }

    @Override
    public void delete(T entity) throws Exception {
        String index = mapping.index(entity);
        String type = mapping.type(entity);
        String id = mapping.id(entity);
        client.prepareDelete(index, type, id).execute().actionGet();
    }

    @Override
    public void deleteByKey(K key) throws Exception {
        String index = mapping.indexForKey(key);
        String type = mapping.typeForKey(key);
        String id = mapping.idForKey(key);
        client.prepareDelete(index, type, id).execute().actionGet();
    }

    @Override
    public void deleteByQuery(Query query) throws Exception {
        String index = mapping.indexForQuery(query);
        String type = mapping.typeForQuery(query);
        DeleteByQueryRequestBuilder builder = new QueryConverter(client, query, index, type).deleteRequest();
        DeleteByQueryResponse response = builder.execute().actionGet();
    }

    @Override
    public boolean exists(K id) throws Exception {
        boolean result = findById(id) != null;
        return result;
    }

    @Override
    public T findById(K key) throws Exception {
        String index = mapping.indexForKey(key);
        String type = mapping.typeForKey(key);
        String id = mapping.idForKey(key);
        try {
            GetResponse response = client.prepareGet(index, type, id).execute().actionGet();
            String source = response.getSourceAsString();
            if (source == null) {
                return null;
            } else {
                T result = mapping.fromDocument(id, source);
                return result;
            }
        } catch (Exception e) {
            log.error("error retrieving dao", e);
            throw e;
        }
    }

    @Override
    public T save(T entity) throws Exception {
        String id = mapping.id(entity);
        String index = mapping.index(entity);
        String type = mapping.type(entity);
        String source = mapping.toDocument(entity);
        client.prepareIndex(index, type, id).setSource(source).execute().actionGet();
        T result = entity;
        return result;
    }

    public Results<T> search(Query query) throws Exception {
        SearchRequestBuilder srb = convert(query);
        SearchResponse response = srb.execute().actionGet();
        Results<T> results = new ResultsConverter<T, K>(query, response, mapping).convert();

        return results;
    }

    @Override
    public long count(String q) throws Exception {
        // hack for now.  eventually use ES search api.
        Query query = new QueryBuilder(q).limit(0).build();
        Results<T> results = search(query);
        long result = results.getTotalItems();
        return result;
    }

    public T findOne(String q) throws Exception {
        T result = null;
        Query query = new QueryBuilder(q).limit(1).build();
        Results<T> results = search(query);
        if (!results.getItems().isEmpty()) {
            result = results.getItems().get(0);
        }
        return result;
    }

    public SearchRequestBuilder convert(Query query) throws Exception {
        String index = mapping.indexForQuery(query);
        String type = mapping.typeForQuery(query);
        return new QueryConverter(client, query, index, type).searchRequest();
    }

    /**
     * Intended only for testing
     */
    public void flush() {
        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    public void createIndex(String indexName) {
    }

    public void recreateMapping(String index, String type, String schema) {

        // drop mapping incase it exists
        try {
            client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();
        } catch (Exception e) {
        }
        // recreate mapping
        client.admin().indices().preparePutMapping("entities").setType("entity").setSource(schema).execute().actionGet();
    }

}
