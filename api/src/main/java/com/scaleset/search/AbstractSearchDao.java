package com.scaleset.search;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSearchDao<T, K> implements GenericSearchDao<T, K> {

    public void close() {
    }

    @Override
    public void delete(T entity) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteByKey(K id) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteByQuery(Query query) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(K id) throws Exception {
        boolean result = findById(id) != null;
        return result;
    }

    @Override
    public T findOne(String q) throws Exception {
        T result = null;
        Query query = new QueryBuilder(q).limit(1).build();
        Results<T> results = search(query);
        if (!results.getItems().isEmpty()) {
            result = results.getItems().get(0);
        }
        return result;
    }

    @Override
    public long count(String q) throws Exception {
        // hack for now.  eventually use ES search api.
        Query query = new QueryBuilder(q).limit(0).build();
        Results<T> results = search(query);
        long result = results.getTotalItems();
        return result;
    }

    @Override
    public T save(T entity) throws Exception {
        throw new UnsupportedOperationException();
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

}
