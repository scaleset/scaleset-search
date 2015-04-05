package com.scaleset.search.mongo.filter;

import com.mongodb.DBObject;
import com.scaleset.search.Filter;

public interface FilterConverter {

    DBObject convert(Filter filter);

}
