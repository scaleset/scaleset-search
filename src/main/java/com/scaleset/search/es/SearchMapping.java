package com.scaleset.search.es;

import com.scaleset.search.Query;
import org.elasticsearch.search.SearchHit;

public interface SearchMapping<T, K> {

    T fromDocument(String id, String doc) throws Exception;

    T fromSearchHit(SearchHit hit) throws Exception;

    String id(T obj) throws Exception;

    String idForKey(K key) throws Exception;

    String index(T object) throws Exception;

    String indexForKey(K key) throws Exception;

    String indexForQuery(Query query) throws Exception;

    String toDocument(T obj) throws Exception;

    String type(T object) throws Exception;

    String typeForKey(K key) throws Exception;

    String typeForQuery(Query query) throws Exception;

}
