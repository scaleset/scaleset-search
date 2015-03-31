package com.scaleset.search.mongo;

import com.mongodb.*;
import com.scaleset.search.AbstractSearchDao;
import com.scaleset.search.Query;
import com.scaleset.search.Results;

import java.util.ArrayList;
import java.util.List;

public class MongoSearchDao<T, K> extends AbstractSearchDao<T, K> {

    private DB db;
    private Class<T> typeClass;
    private SearchMapping<T, K> searchMapping;

    public MongoSearchDao(DB db, SearchMapping<T, K> searchMapping) {
        this.db = db;
        this.searchMapping = searchMapping;
    }

    @Override
    public void close() {
    }

    @Override
    public Results<T> search(Query query) throws Exception {
        SchemaMapper schemaMapper = searchMapping.schemaMapperForQuery(query);
        MongoQueryConverter<T> converter = new MongoQueryConverter<T>(schemaMapper);
        DBCollection collection = db.getCollection(searchMapping.collectionForQuery(query));

        DBObject mongoQ = null;

        List<DBObject> filters = new ArrayList<>();
        converter.addQ(query, filters);
        converter.addFilters(query, filters);
        if (filters.size() > 1) {

            mongoQ = new BasicDBObject("$and", filters);
        } else if (filters.size() == 1) {
            mongoQ = filters.get(0);
        } else {
            mongoQ = new BasicDBObject();
        }
        DBCursor cursor = collection.find(mongoQ);

        converter.addPaging(query, cursor);
        converter.addSorts(query, cursor);
        Results<T> results = new ResultsConverter<T, K>(query, cursor, searchMapping).convert();
        return results;
    }

    @Override
    public T findById(K key) throws Exception {
        String id = searchMapping.idForKey(key);
        DBCollection collection = db.getCollection(searchMapping.collectionForKey(key));
        DBObject object = collection.findOne(new BasicDBObject("_id", id));
        T result = searchMapping.fromDocument(id, object);
        return result;
    }

    @Override
    public List<T> saveBatch(List<T> entities) throws Exception {
        List<T> results = new ArrayList<>();
        for (T entity : entities) {
            save(entity);
            results.add(entity);
        }
        return results;
    }

    @Override
    public void delete(T entity) throws Exception {
        DBCollection collection = db.getCollection(searchMapping.collection(entity));

        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteByKey(K id) throws Exception {
        DBCollection collection = db.getCollection(searchMapping.collectionForKey(id));
        collection.remove(new BasicDBObject("_id", id));
    }

    @Override
    public void deleteByQuery(Query query) throws Exception {
        //collection.remove(query.getQ());
    }

    @Override
    public T save(T entity) throws Exception {
        DBCollection collection = db.getCollection(searchMapping.collection(entity));
        DBObject object = searchMapping.toDocument(entity);
        collection.save(object);
        return entity;
    }

}
