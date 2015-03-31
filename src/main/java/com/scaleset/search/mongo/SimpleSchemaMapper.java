package com.scaleset.search.mongo;

import com.scaleset.utils.Coerce;
import org.apache.lucene.util.BytesRef;

import java.util.HashMap;
import java.util.Map;

public class SimpleSchemaMapper implements SchemaMapper {

    private String defaultField;

    private Map<String, Class<?>> simpleSchema = new HashMap<>();

    public SimpleSchemaMapper(String defaultField) {
        this.defaultField = defaultField;
    }

    @Override
    public String mapField(String field) {
        return field;
    }

    public SimpleSchemaMapper withMapping(String field, Class<?> type) {
        simpleSchema.put(field, type);
        return this;
    }

    @Override
    public Object mapValue(String field, Object value) {
        Class<?> type = simpleSchema.get(field);
        if (type == null) {
            type = String.class;
        }
        if (value instanceof BytesRef) {
            value = ((BytesRef) value).utf8ToString();
        }
        return Coerce.to(value, type);
    }

    @Override
    public String defaultField() {
        return defaultField;
    }
}
