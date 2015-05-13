package com.scaleset.search.mongo;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Query;
import com.scaleset.utils.Coerce;

import java.lang.reflect.ParameterizedType;
import java.util.function.Function;

public abstract class AbstractSearchMapping<T, K> implements SearchMapping<T, K> {

    private String defaultCollection;
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new GeoJsonModule());
    private JavaType javaType;
    private SchemaMapper schemaMapper = new SimpleSchemaMapper("text");
    private Function<K, String> keyMapper;
    private Function<T, String> idMapper;

    public AbstractSearchMapping(String defaultCollection) {
        this.defaultCollection = defaultCollection;
        Class<T> persistentClass = (Class<T>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];
        this.javaType = objectMapper.getTypeFactory().constructType(persistentClass);
    }

    public AbstractSearchMapping(String defaultCollection, Function<T, String> idMapper, Function<K, String> keyMapper) {
        this.defaultCollection = defaultCollection;
        Class<T> persistentClass = (Class<T>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];
        this.javaType = objectMapper.getTypeFactory().constructType(persistentClass);
        this.idMapper = idMapper;
        this.keyMapper = keyMapper;
    }

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
    public String id(T obj) throws Exception {
        if (idMapper != null) {
            return idMapper.apply(obj);
        } else {
            throw new RuntimeException("No idMapper registered");
        }
    }

    @Override
    public String idForKey(K key) throws Exception {
        if (keyMapper != null) {
            return keyMapper.apply(key);
        } else {
            return Coerce.toString(key);
        }
    }

    protected AbstractSearchMapping<T, K> idMapper(Function<T, String> idMapper) {
        this.idMapper = idMapper;
        return this;
    }

    protected AbstractSearchMapping<T, K> keyMapper(Function<K, String> keyMapper) {
        this.keyMapper = keyMapper;
        return this;
    }

    protected AbstractSearchMapping<T, K> objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    protected AbstractSearchMapping<T, K> schemaMapper(SchemaMapper schemaMapper) {
        this.schemaMapper = schemaMapper;
        return this;
    }

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
