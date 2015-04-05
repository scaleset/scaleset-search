package com.scaleset.search.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.scaleset.search.Filter;
import com.scaleset.search.Query;
import com.scaleset.search.Sort;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoQueryConverter<T> {

    private SchemaMapper schemaMapper;
    private Iterator<Object> params;

    public MongoQueryConverter(SchemaMapper schemaMapper, Object... params) {
        this.schemaMapper = schemaMapper;
        this.params = Arrays.asList(params).iterator();
    }

    public DBObject convertQ(String q) {
        if ("{}".equals(q)) {
            return new BasicDBObject();
        }
        DBObject result;
        if (q == null || "".equals(q.trim())) {
            result = new BasicDBObject();
        } else {
            org.apache.lucene.search.Query luceneQuery = parseConstraint(q);
            result = handleQuery(luceneQuery);
        }
        return result;
    }

    public void addQ(Query query, List<DBObject> queries) {
        String q = query.getQ();
        if (q != null && !q.isEmpty()) {
            queries.add(convertQ(q));
        }
    }

    public void addFilters(Query query, List<DBObject> filters) {
        for (Filter filter : query.getFilters().values()) {
            if ("query".equals(filter.getType())) {
                String q = filter.getString("query");
                DBObject f = convertQ(q);
                filters.add(f);
            }
        }
    }

    public void addPaging(Query query, DBCursor cursor) {
        cursor.skip(query.getOffset());
        cursor.limit(query.getLimit());
    }

    public void addSorts(Query query, DBCursor cursor) {
        DBObject sortObject = convertSorts(query.getSorts());
        cursor.sort(sortObject);
    }

    protected DBObject convertSorts(Sort... sorts) {
        BasicDBObject orders = new BasicDBObject();
        if (sorts != null && sorts.length > 0) {
            for (Sort sort : sorts) {
                List<String> fields = getField(sort.getField());
                for (String field : fields) {
                    int direction = 1;
                    if (Sort.Direction.Descending == sort.getDirection()) {
                        direction = -1;
                    }
                    orders.append(field, direction);
                }
            }
        }
        return orders;
    }

    protected DBObject handleQuery(org.apache.lucene.search.Query query, boolean prohibited) {
        return prohibited ? handleProhibitedQuery(query) : handleQuery(query);
    }

    protected DBObject handleQuery(org.apache.lucene.search.Query query) {
        DBObject result = null;
        if (query instanceof TermQuery) {
            result = handleTermQuery((TermQuery) query);
        } else if (query instanceof BooleanQuery) {
            result = handleBooleanQuery((BooleanQuery) query, false);
        } else if (query instanceof TermRangeQuery) {
            result = handleRangeQuery((TermRangeQuery) query);
        } else if (query instanceof PrefixQuery) {
            result = handlePrefixQuery((PrefixQuery) query);
        } else if (query instanceof WildcardQuery) {
            result = handleWildcardQuery((WildcardQuery) query);
        } else if (query instanceof PhraseQuery) {
            result = handlePhraseQuery((PhraseQuery) query);
        } else {
            System.out.println("query: " + query);
        }
        return result;
    }

    protected DBObject handleProhibitedQuery(org.apache.lucene.search.Query query) {
        DBObject result = null;
        if (query instanceof TermQuery) {
            result = handleProhibitedTermQuery((TermQuery) query);
        } else if (query instanceof WildcardQuery) {
            result = handleProhibitedWildcardQuery((WildcardQuery) query);
        } else if (query instanceof BooleanQuery) {
            result = handleBooleanQuery((BooleanQuery) query, true);
        } else {
            throw new RuntimeException("Unsupported query: " + query);
        }
        return result;
    }

    protected DBObject handleBooleanQuery(BooleanQuery boolQuery, boolean prohibited) {
        // De Morgan's Law:
        // "not (A and B)" is the same as "(not A) or (not B)"
        // "not (A or B)" is the same as "(not A) and (not B)".
        //
        DBObject result = null;
        List<BooleanClause> clauses = boolQuery.clauses();
        int nClauses = clauses.size();
        List children = new BasicDBList();

        int orCount = 0;
        int andCount = 0;
        int notCount = 0;
        for (int i = 0; i < nClauses; ++i) {
            BooleanClause clause = clauses.get(i);
            if (clause.isRequired()) {
                ++andCount;
            } else if (clause.isProhibited()) {
                ++notCount;
            } else {
                ++orCount;
            }
            // inverse if our boolean clause is prohibited as a whole
            boolean prohibit = prohibited ? !clause.isProhibited() : clause.isProhibited();
            children.add(handleQuery(clause.getQuery(), prohibit));
        }
        if (andCount > 0 && orCount > 0) {
            throw new RuntimeException("Mixed boolean clauses not supported!");
        }
        if (children.size() == 1) {
            result = (DBObject) children.get(0);
        } else {
            boolean conjunction = (andCount > 1 || notCount > 1);
            // inverse is out boolean clause is prohibited as a whole
            if (prohibited) {
                conjunction = !conjunction;
            }
            String operator = conjunction ? "$and" : "$or";
            result = new BasicDBObject(operator, children);
        }
        return result;
    }

    protected DBObject handleTermQuery(TermQuery termQuery) {
        List<DBObject> results = new ArrayList<>();
        Term term = termQuery.getTerm();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            Object value = parse(field, term.text());
            results.add(new BasicDBObject(field, value));
        }
        return join("$or", results);
    }

    protected DBObject handleProhibitedTermQuery(TermQuery termQuery) {
        List<DBObject> results = new ArrayList<>();
        Term term = termQuery.getTerm();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            Object value = parse(field, term.text());
            results.add(new BasicDBObject(field, new BasicDBObject("$ne", value)));
        }
        return join("$and", results);
    }

    protected DBObject handlePrefixQuery(PrefixQuery prefixQuery) {
        List<DBObject> results = new ArrayList<>();
        Term term = prefixQuery.getPrefix();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            Object value = parse(field, term.text());
            Pattern pattern = Pattern.compile("^" + Matcher.quoteReplacement(value.toString()) + ".*", Pattern.CASE_INSENSITIVE);
            results.add(new BasicDBObject(field, pattern));
        }
        return join("$or", results);
    }

    protected DBObject handleWildcardQuery(WildcardQuery wildcardQuery) {
        Term term = wildcardQuery.getTerm();
        List<DBObject> results = new ArrayList<>();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            String value = term.text().replace("?", "_QUESTION_MARK_").replace("*", "_STAR_");
            value = Matcher.quoteReplacement(value);
            value = value.replace("_QUESTION_MARK_", ".?").replace("_STAR_", ".*");
            Pattern pattern = Pattern.compile("^" + Matcher.quoteReplacement(value) + "$", Pattern.CASE_INSENSITIVE);
            results.add(new BasicDBObject(field, pattern));
        }
        return join("$or", results);
    }

    protected DBObject handleProhibitedWildcardQuery(WildcardQuery wildcardQuery) {
        Term term = wildcardQuery.getTerm();
        List<DBObject> results = new ArrayList<>();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            String pattern = term.text().replace("?", "_QUESTION_MARK_").replace("*", "_STAR_");
            // escape in case pattern contains some regex special characters
            pattern = Matcher.quoteReplacement(pattern);
            pattern = pattern.replace("_QUESTION_MARK_", ".?").replace("_STAR_", ".*");
            results.add(new BasicDBObject(field, new BasicDBObject("$ne", ("^" + pattern + "$"))));
        }
        return join("$and", results);
    }

    protected DBObject handleRangeQuery(TermRangeQuery rangeQuery) {
        List<DBObject> results = new ArrayList<>();
        List<String> fields = getField(rangeQuery.getField());
        for (String field : fields) {
            Object lower = parse(field, rangeQuery.getLowerTerm());
            Object upper = parse(field, rangeQuery.getUpperTerm());
            DBObject expression = new BasicDBObject();

            if (upper != null) {
                expression.put(rangeQuery.includesUpper() ? "$lte" : "$lt", upper);
            }
            if (lower != null) {
                expression.put(rangeQuery.includesLower() ? "$gte" : "$gt", lower);
            }
            results.add(new BasicDBObject(field, expression));
        }
        return join("$or", results);
    }

    protected DBObject handlePhraseQuery(PhraseQuery phraseQuery) {
        Term[] terms = phraseQuery.getTerms();
        List<String> fields = getField(phraseQuery.getTerms()[0].field());
        String phrase = null;
        for (Term term : terms) {
            if (phrase == null) {
                phrase = term.text();
            } else {
                phrase += " " + term.text();
            }
        }
        List<DBObject> results = new ArrayList<>();
        for (String field : fields) {
            String value = parse(field, phrase) + "";
            Pattern pattern = Pattern.compile(Matcher.quoteReplacement(value), Pattern.CASE_INSENSITIVE);
            results.add(new BasicDBObject(field, pattern));
        }
        return join("$or", results);
    }

    DBObject join(String operator, List<DBObject> items) {
        if (items.size() == 1) {
            return items.get(0);
        } else {
            return new BasicDBObject(operator, items);
        }
    }

    protected List<String> getField(String field) {
        return schemaMapper.mapField(field);
    }

    protected Object parse(String field, Object value) {
        if ("#".equals(value)) {
            value = params.next();
        }
        return schemaMapper.mapValue(field, value);
    }

    protected org.apache.lucene.search.Query parseConstraint(String q) {

        Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_4_9);
        QueryParser parser = new QueryParser(Version.LUCENE_4_9, schemaMapper.defaultField(), analyzer);
        parser.setDefaultOperator(QueryParser.Operator.OR);
        parser.setAllowLeadingWildcard(true);
        try {
            org.apache.lucene.search.Query result = parser.parse(q);
            return result;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
