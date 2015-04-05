package com.scaleset.search.mongo;

import org.jongo.query.QueryFactory;

public class LuceneJongoQueryFactory implements QueryFactory {

    @Override
    public org.jongo.query.Query createQuery(String query, Object... parameters) {
        return new LuceneJongoQuery(query, parameters);
    }

}
