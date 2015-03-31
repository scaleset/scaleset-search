package com.scaleset.search.mongo;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Query;

public abstract class AbstractSearchMapping<T, K> implements SearchMapping<T, K> {

    private String defaultCollection;
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new GeoJsonModule());
    private JavaType javaType;
    private SchemaMapper schemaMapper = new SimpleSchemaMapper("text");

    public AbstractSearchMapping(Class<? extends T> type, String defaultCollection) {
        this(type, defaultCollection, new SimpleSchemaMapper("text"));
    }

    public AbstractSearchMapping(Class<? extends T> type, String defaultCollection, SchemaMapper schemaMapper) {
        this.javaType = objectMapper.getTypeFactory().constructType(type);
        this.defaultCollection = defaultCollection;
        this.schemaMapper = schemaMapper;
    }

    @Override
    public String collection(T object) throws Exception {
        return defaultCollection;
    }

    @Override
    public String collectionForKey(K key) throws Exception {
        return defaultCollection;
    }

    @Override
    public String collectionForQuery(Query query) throws Exception {
        return defaultCollection;
    }

    @Override
    public T fromDocument(String id, DBObject doc) throws Exception {
        T result = objectMapper.convertValue(doc, javaType);
        return result;
    }

    @Override
    public abstract String id(T obj) throws Exception;

    @Override
    public abstract String idForKey(K key) throws Exception;

    @Override
    public DBObject toDocument(T obj) throws Exception {
        DBObject result = objectMapper.convertValue(obj, BasicDBObject.class);
        return result;
    }

    @Override
    public SchemaMapper schemaMapperForQuery(Query query) throws Exception {
        return schemaMapper;
    }

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    protected SchemaMapper getSchemaMapper() {
        return schemaMapper;
    }
}
