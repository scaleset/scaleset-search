package com.scaleset.search.mongo;

public interface SchemaMapper {

    String mapField(String field);

    Object mapValue(String field, Object value);

    String defaultField();
}
