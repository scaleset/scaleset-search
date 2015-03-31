package com.scaleset.search.mongo;

import com.mongodb.DBObject;
import com.scaleset.search.Query;

public interface SearchMapping<T, K> {

    String collection(T object) throws Exception;

    String collectionForKey(K key) throws Exception;

    String collectionForQuery(Query query) throws Exception;

    T fromDocument(String id, DBObject doc) throws Exception;

    String id(T obj) throws Exception;

    String idForKey(K key) throws Exception;

    DBObject toDocument(T obj) throws Exception;

    SchemaMapper schemaMapperForQuery(Query query) throws Exception;
}
