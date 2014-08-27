package com.scaleset.search.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.scaleset.search.Query;
import com.scaleset.search.Sort;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

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
                String field = getField(sort.getField());
                int direction = 1;
                if (Sort.Direction.Descending == sort.getDirection()) {
                    direction = -1;
                }
                orders.append(field, direction);
            }
        }
        return new BasicDBObject("$orderBy", orders);
    }

    protected DBObject handleQuery(org.apache.lucene.search.Query query) {
        DBObject result = null;
        if (query instanceof TermQuery) {
            result = handleTermQuery((TermQuery) query);
        } else if (query instanceof BooleanQuery) {
            result = handleBooleanQuery((BooleanQuery) query);
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
        } else {
            throw new RuntimeException("Unsupported query: " + query);
        }
        return result;
    }

    protected DBObject handleBooleanQuery(BooleanQuery boolQuery) {
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
            if (clause.isProhibited()) {
                children.add(handleProhibitedQuery(clause.getQuery()));
            } else {
                children.add(handleQuery(clause.getQuery()));
            }
        }
        if (andCount > 0 && orCount > 0) {
            throw new RuntimeException("Mixed boolean clauses not supported!");
        }
        if (andCount > 1 || notCount > 1) {
            result = new BasicDBObject("$and", children);
        } else if (orCount > 1) {
            result = new BasicDBObject("$or", children);
        } else if (andCount == 1 || orCount == 1 || nClauses == 1) {
            result = (DBObject) children.get(0);
        }
        return result;
    }

    protected DBObject handleTermQuery(TermQuery termQuery) {
        Term term = termQuery.getTerm();
        String field = getField(term.field());
        Object value = parse(field, term.text());
        return new BasicDBObject(field, value);
    }

    protected DBObject handleProhibitedTermQuery(TermQuery termQuery) {
        Term term = termQuery.getTerm();
        String field = getField(term.field());
        Object value = parse(field, term.text());
        return new BasicDBObject(field, new BasicDBObject("$ne", value));
    }

    protected DBObject handlePrefixQuery(PrefixQuery prefixQuery) {
        Term term = prefixQuery.getPrefix();
        String field = getField(term.field());
        Object value = parse(field, term.text());
        Pattern pattern = Pattern.compile("^" + Matcher.quoteReplacement(value.toString()) + ".*");
        return new BasicDBObject(field, pattern);
    }

    protected DBObject handleWildcardQuery(WildcardQuery wildcardQuery) {
        Term term = wildcardQuery.getTerm();
        String field = getField(term.field());
        String pattern = term.text().replace("?", "_QUESTION_MARK_").replace("*", "_STAR_");
        pattern = Matcher.quoteReplacement(pattern);
        pattern = pattern.replace("_QUESTION_MARK_", ".?").replace("_STAR_", ".*");
        return new BasicDBObject(field, "^" + pattern + "$");
    }

    protected DBObject handleProhibitedWildcardQuery(WildcardQuery wildcardQuery) {
        Term term = wildcardQuery.getTerm();
        String field = getField(term.field());
        String pattern = term.text().replace("?", "_QUESTION_MARK_").replace("*", "_STAR_");
        // escape in case pattern contains some regex special characters
        pattern = Matcher.quoteReplacement(pattern);
        pattern = pattern.replace("_QUESTION_MARK_", ".?").replace("_STAR_", ".*");
        return new BasicDBObject(field, new BasicDBObject("$ne", ("^" + pattern + "$")));
    }


    protected DBObject handleRangeQuery(TermRangeQuery rangeQuery) {
        String field = getField(rangeQuery.getField());
        Object lower = parse(field, rangeQuery.getLowerTerm());
        Object upper = parse(field, rangeQuery.getUpperTerm());
        DBObject result = new BasicDBObject();

        if (upper != null) {
            result.put(field, new BasicDBObject(rangeQuery.includesUpper() ? "$lte" : "$lt", upper));
        }
        if (lower != null) {
            result.put(field, new BasicDBObject(rangeQuery.includesLower() ? "$gte" : "$gt", upper));
        }
        return result;
    }

    protected DBObject handlePhraseQuery(PhraseQuery phraseQuery) {
        Term[] terms = phraseQuery.getTerms();
        String field = getField(phraseQuery.getTerms()[0].field());
        String phrase = "";
        for (Term term : terms) {
            phrase += " " + term.text();
        }
        String value = parse(field, phrase) + "";
        Pattern pattern = Pattern.compile(Matcher.quoteReplacement(value), Pattern.CASE_INSENSITIVE);
        return new BasicDBObject(field, pattern);
    }

    protected String getField(String field) {
        if ("#".equals(field)) {
            Object param = params.next();
            field = param.toString();
        }
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
