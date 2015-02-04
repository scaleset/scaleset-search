package com.scaleset.search.es;

import java.util.List;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scaleset.geo.geojson.GeoJsonModule;
import com.scaleset.search.Query;

public abstract class AbstractSearchMapping<T, K> implements SearchMapping<T, K> {

    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new GeoJsonModule());
    private JavaType javaType;
    private String defaultIndex;
    private String[] indices;
    private String[] defaultTypes;
    private JsonNodeFactory nodeFactory = JsonNodeFactory.instance;

    public AbstractSearchMapping(Class<? extends T> type, String defaultIndex, String... defaultTypes) {
        this.javaType = objectMapper.getTypeFactory().constructType(type);
        this.defaultIndex = defaultIndex;
        this.defaultTypes = defaultTypes;
        this.indices = new String[] { defaultIndex };
    }

    public AbstractSearchMapping(Class<? extends T> type, String[] indices, String defaultIndex, String... defaultTypes) {
        this.javaType = objectMapper.getTypeFactory().constructType(type);
        this.defaultIndex = defaultIndex;
        this.defaultTypes = defaultTypes;
        this.indices = indices;
    }

    public AbstractSearchMapping(Class<? extends T> type, String[] indices, String... defaultTypes) {
        this.javaType = objectMapper.getTypeFactory().constructType(type);
        this.defaultTypes = defaultTypes;
        this.indices = indices;
        this.defaultIndex = indices[0];
    }

    public AbstractSearchMapping(TypeReference typeReference, String defaultIndex, String... defaultTypes) {
        this.javaType = objectMapper.getTypeFactory().constructType(typeReference);
        this.defaultIndex = defaultIndex;
        this.defaultTypes = defaultTypes;
        this.indices = new String[] { defaultIndex };
    }

    public AbstractSearchMapping(TypeReference typeReference, String[] indices, String defaultIndex, String... defaultTypes) {
        this.javaType = objectMapper.getTypeFactory().constructType(typeReference);
        this.defaultIndex = defaultIndex;
        this.defaultTypes = defaultTypes;
        this.indices = indices;
    }

    public AbstractSearchMapping(TypeReference typeReference, String[] indices, String... defaultTypes) {
        this.javaType = objectMapper.getTypeFactory().constructType(typeReference);
        this.defaultTypes = defaultTypes;
        this.indices = indices;
        this.defaultIndex = indices[0];
    }

    public T fromDocument(String id, String doc) throws Exception {
        T obj = objectMapper.readValue(doc, javaType);
        return obj;
    }

    public T fromSearchHit(SearchHit searchHit) {
        ObjectNode json = nodeFactory.objectNode();
        for (SearchHitField field : searchHit.fields().values()) {
            putField(json, field);
        }
        T result = objectMapper.convertValue(json, javaType);
        return result;
    }

    void putField(ObjectNode json, SearchHitField field) {
        String fieldName = field.getName();
        String[] nameParts = fieldName.split("\\.");
        String property = nameParts[nameParts.length - 1];
        ObjectNode obj = json;
        for (int i = 0; i < nameParts.length - 1; ++i) {
            String part = nameParts[i];
            obj = obj.with(part);
        }
        List<Object> values = field.getValues();
        Object value = values.size() > 1 ? values : values.get(0);
        obj.put(property, nodeFactory.pojoNode(value));
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
    public String[] indicesForQuery(Query query) throws Exception {
        return indices;
    }

    @Override
    public String toDocument(T obj) throws Exception {
        String result = objectMapper.writeValueAsString(obj);
        return result;
    }

    @Override
    public String type(T object) throws Exception {
        return defaultTypes.length == 0 ? null : defaultTypes[0];
    }

    @Override
    public String typeForKey(K key) throws Exception {
        return defaultTypes.length == 0 ? null : defaultTypes[0];
    }

    @Override
    public String[] typesForQuery(Query query) throws Exception {
        return defaultTypes;
    }
}
