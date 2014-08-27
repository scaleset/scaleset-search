package com.scaleset.search.mongo;

import com.scaleset.search.AggregationResults;
import com.scaleset.search.Query;
import com.scaleset.search.Results;
import org.jongo.Find;
import org.jongo.MongoCursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ResultsConverter<T, K> {

    private Query query;
    private Find find;
    private int totalItems;
    private List<T> items = new ArrayList<>();
    private List<AggregationResults> aggregationResults = new ArrayList<>();
    private Class<T> typeClass;

    public ResultsConverter(Query query, Find find, Class<T> typeClass) {
        this.query = query;
        this.find = find;
        this.typeClass = typeClass;
    }

    protected void initialize() throws IOException {
        MongoCursor<T> cursor = null;
        totalItems = cursor.count();
        while (cursor.hasNext()) {
            items.add(cursor.next());
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
