package com.scaleset.search.mongo;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.scaleset.search.AggregationResults;
import com.scaleset.search.Query;
import com.scaleset.search.Results;
import com.scaleset.utils.Coerce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultsConverter<T, K> {

    private Query query;
    private DBCursor cursor;
    private int totalItems;
    private List<T> items = new ArrayList<>();
    private Map<String, AggregationResults> aggregationResults = new HashMap<>();
    SearchMapping<T, K> searchMapping;

    public ResultsConverter(Query query, DBCursor cursor, SearchMapping<T, K> searchMapping) {
        this.query = query;
        this.cursor = cursor;
        this.searchMapping = searchMapping;
    }

    protected void initialize() throws IOException {
        totalItems = cursor.count();
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            String id = Coerce.toString(obj.get("_id"));
            try {
                T item = searchMapping.fromDocument(id, obj);
                items.add(item);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cursor.close();
    }

    protected void addAggResults() {
        // aggs not yet supported
    }

    public Results<T> convert() throws Exception {
        initialize();
        addAggResults();
        Results<T> results = new Results<T>(query, aggregationResults, items, totalItems);
        return results;
    }

}
