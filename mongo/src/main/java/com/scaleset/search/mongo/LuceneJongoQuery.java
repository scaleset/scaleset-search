package com.scaleset.search.mongo;

import com.mongodb.DBObject;

class LuceneJongoQuery implements org.jongo.query.Query {

    private String query;
    private Object[] parameters;

    LuceneJongoQuery(String query, Object[] parameters) {
        this.query = query;
        this.parameters = parameters;
    }

    @Override
    public DBObject toDBObject() {
        return new MongoQueryConverter(new SimpleSchemaMapper("text"), parameters).convertQ(query);
    }
}
