package com.scaleset.search.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Query;

public abstract class AbstractSearchMapping<T, K> implements SearchMapping<T, K> {

    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new GeoJsonModule());
    private JavaType javaType;
    private String defaultIndex;
    private String defaultType;

    public AbstractSearchMapping(Class<? extends T> type, String defaultIndex, String defaultType) {
        this.javaType = objectMapper.getTypeFactory().constructType(type);
        this.defaultIndex = defaultIndex;
        this.defaultType = defaultType;
    }

    public AbstractSearchMapping(TypeReference typeReference, String defaultIndex, String defaultType) {
        this.javaType = objectMapper.getTypeFactory().constructType(typeReference);
        this.defaultIndex = defaultIndex;
        this.defaultType = defaultType;
    }

    public T fromDocument(String id, String doc) throws Exception {
        T obj = objectMapper.readValue(doc, javaType);
        return obj;
    }

    @Override
    public abstract String id(T obj) throws Exception;

    @Override
    public abstract String idForKey(K key) throws Exception;

    @Override
    public String index(T object) throws Exception {
        return defaultIndex;
    }

    @Override
    public String indexForKey(K key) throws Exception {
        return defaultIndex;
    }

    @Override
    public String indexForQuery(Query query) throws Exception {
        return defaultIndex;
    }

    @Override
    public String toDocument(T obj) throws Exception {
        String result = objectMapper.writeValueAsString(obj);
        return result;
    }

    @Override
    public String type(T object) throws Exception {
        return defaultType;
    }

    @Override
    public String typeForKey(K key) throws Exception {
        return defaultType;
    }

    @Override
    public String typeForQuery(Query query) throws Exception {
        return defaultType;
    }
}
