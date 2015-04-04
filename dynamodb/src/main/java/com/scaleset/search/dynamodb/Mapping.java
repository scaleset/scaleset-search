package com.scaleset.search.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;

public interface Mapping<T, ID> {

    Map<String, AttributeValue> entityToKey(T obj) throws Exception;

    T fromRow(Map<String, AttributeValue> row) throws Exception;

    Map<String, AttributeValue> idToKey(ID id) throws Exception;

    Map<String, AttributeValue> toRow(T obj) throws Exception;

}
