package com.scaleset.search.pojo;

import com.scaleset.search.GenericSearchDao;
import com.scaleset.search.Query;
import com.scaleset.search.QueryBuilder;
import com.scaleset.search.Results;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MapBackedSearchDao<T, ID> implements GenericSearchDao<T, ID> {

    private Map<ID, T> items;

    private Function<T, ID> idFunction;
    private SchemaMapper schemaMapper = new SimpleSchemaMapper("text");

    public MapBackedSearchDao(Function<T, ID> idFunction) {
        this.idFunction = idFunction;
        items = new HashMap<>();
    }

    public MapBackedSearchDao(Function<T, ID> idFunction, Map<ID, T> items) {
        this.idFunction = idFunction;
        this.items = items;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void delete(T entity) throws Exception {
        items.remove(id(entity));
    }

    @Override
    public void deleteByKey(ID id) throws Exception {
        items.remove(id);
    }

    @Override
    public void deleteByQuery(Query query) throws Exception {
        Results<T> results = search(query);
        for (T item : results.getItems()) {
            delete(item);
        }
    }

    @Override
    public boolean exists(ID id) throws Exception {
        return items.containsKey(id);
    }

    @Override
    public Results<T> search(Query query) throws Exception {
        LuceneExpressionConverter converter = new LuceneExpressionConverter(schemaMapper);
        Predicate<T> predicate = converter.convertQ(query.getQ());
        List resultItems = items.values().stream().filter(predicate).collect(Collectors.toList());
        int totalItems = resultItems.size();

        // apply paging
        int fromIndex = query.getOffset();
        int toIndex = fromIndex + query.getLimit();
        if (fromIndex > resultItems.size()) {
            resultItems = Collections.emptyList();
        } else {
            toIndex = Math.min(resultItems.size(), toIndex);
            resultItems = resultItems.subList(fromIndex, toIndex);
        }
        Results<T> results = new Results<>(query, null, resultItems, totalItems);
        return results;
    }


    @Override
    public long count(String q) throws Exception {
        Query query = new QueryBuilder().limit(0).q(q).build();
        return search(query).getTotalItems();
    }

    @Override
    public T findById(ID id) throws Exception {
        return items.get(id);
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
    public T save(T entity) throws Exception {
        return items.put(id(entity), entity);
    }

    @Override
    public List<T> saveBatch(List<T> entities) throws Exception {
        for (T entity : entities) {
            save(entity);
        }
        return entities;
    }

    ID id(T entity) {
        return idFunction.apply(entity);
    }

}
