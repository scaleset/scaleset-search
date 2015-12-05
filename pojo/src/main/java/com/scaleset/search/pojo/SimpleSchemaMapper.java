package com.scaleset.search.pojo;

import com.scaleset.utils.Coerce;
import org.apache.lucene.util.BytesRef;

import java.util.*;

public class SimpleSchemaMapper implements SchemaMapper {

    private String defaultField;
    private Map<String, List<String>> aliases = new HashMap<>();

    private Map<String, Class<?>> simpleSchema = new HashMap<>();

    public SimpleSchemaMapper(String defaultField) {
        this.defaultField = defaultField;
    }

    @Override
    public String defaultField() {
        return defaultField;
    }

    @Override
    public List<String> mapField(String field) {
        List<String> result = aliases.get(field);
        if (result == null || result.isEmpty()) {
            result = Collections.singletonList(field);
        }
        return result;
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

    public SimpleSchemaMapper withMapping(String field, Class<?> type) {
        simpleSchema.put(field, type);
        return this;
    }

    public SimpleSchemaMapper withAlias(String field, String... fields) {
        aliases.put(field, Arrays.asList(fields));
        return this;
    }
}
