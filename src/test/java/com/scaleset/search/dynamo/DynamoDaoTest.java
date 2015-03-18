package com.scaleset.search.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DynamoDaoTest extends Assert {

    private AmazonDynamoDB dynamo;
    private DaoSearchDynamo<String, Object> dao;

    @Before
    public void setUp() {
        dynamo = new AmazonDynamoDBClient();
        String tableName = "record_test_table";
        Mapping<Object, String> mapping = null;

        dao = new DaoSearchDynamo(dynamo, tableName, mapping);
    }

    @Test
    public void testDynamoDao() throws Exception {
    }
}
