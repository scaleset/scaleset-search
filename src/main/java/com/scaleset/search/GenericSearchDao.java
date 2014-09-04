package com.scaleset.search;

import com.scaleset.search.Query;
import com.scaleset.search.Results;

import java.util.List;

public interface GenericSearchDao<T, KEY> {

    void delete(T entity) throws Exception;

    void deleteByKey(KEY id) throws Exception;

    void deleteByQuery(Query query) throws Exception;

    boolean exists(KEY id) throws Exception;

    Results<T> search(Query query) throws Exception;

    long count(String q) throws Exception;

    T findById(KEY id) throws Exception;

    T findOne(String q) throws Exception;

    T save(T entity) throws Exception;

    List<T> saveBatch(List<T> entities) throws Exception;

}
