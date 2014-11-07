package com.scaleset.search.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.scaleset.search.AbstractSearchDao;
import com.scaleset.search.Query;
import com.scaleset.search.QueryBuilder;
import com.scaleset.search.Results;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DaoSearchDynamo<T, ID> extends AbstractSearchDao<T, ID> {

    private Mapping<T, ID> mapping;
    private AmazonDynamoDB dynamoDb;
    private String tableName;

    public DaoSearchDynamo(AmazonDynamoDB dynamoDB, String tableName, Mapping<T, ID> mapping) {
        this.dynamoDb = dynamoDB;
        this.tableName = tableName;
        this.mapping = mapping;
    }

    @Override
    public void delete(T entity) throws Exception {
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(mapping.entityToKey(entity));
        dynamoDb.deleteItem(request);
    }

    @Override
    public void deleteByQuery(Query query) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteByKey(ID id) throws Exception {
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(mapping.idToKey(id));
        dynamoDb.deleteItem(request);
    }

    @Override
    public Results<T> search(Query query) throws Exception {
        throw new UnsupportedOperationException("Count not supported");
    }

    @Override
    public long count(String q) throws Exception {
        throw new UnsupportedOperationException("Count not supported");
    }

    public Results<T> findAll(int limit) throws Exception {
        List<T> items = new ArrayList<>();
        ScanRequest scanRequest = new ScanRequest(tableName);
        scanRequest.withLimit(limit);
        ScanResult scanResult = dynamoDb.scan(scanRequest);
        for (Map<String, AttributeValue> item : scanResult.getItems()) {
            items.add(mapping.fromRow(item));
        }
        Results<T> results = new Results<T>(new QueryBuilder().limit(limit).build(), null, items, items.size());
        return results;
    }

    @Override
    public T findById(ID id) throws Exception {
        GetItemResult getResult = dynamoDb.getItem(new GetItemRequest().withTableName(tableName).withKey(mapping.idToKey(id)));
        T result = mapping.fromRow(getResult.getItem());
        return result;
    }

    protected AmazonDynamoDB getDynamoDb() {
        return dynamoDb;
    }

    protected Mapping<T, ID> getMapping() {
        return mapping;
    }

    /**
     * TODO: Identity management strategy
     */
    @Override
    public T save(T entity) throws Exception {
        Map<String, AttributeValue> item = mapping.toRow(entity);
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        dynamoDb.putItem(putItemRequest);
        return entity;
    }

}
