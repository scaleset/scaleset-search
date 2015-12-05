package com.scaleset.search.pojo;

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
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.scaleset.search.pojo.Functions.*;

public class LuceneExpressionConverter {

    private SchemaMapper schemaMapper;

    private Iterator<Object> params;

    public LuceneExpressionConverter(SchemaMapper schemaMapper, Object... params) {
        this.schemaMapper = schemaMapper;
        this.params = Arrays.asList(params).iterator();
    }

    Predicate convertQ(String q) {
        Predicate<Object> result;

        if (q == null || "".equals(q.trim())) {
            result = (obj) -> true;
        } else {
            org.apache.lucene.search.Query luceneQuery = parseConstraint(q);
            result = handleQuery(luceneQuery);
        }
        return result;
    }

    protected Predicate handleQuery(org.apache.lucene.search.Query query, boolean prohibited) {
        return prohibited ? handleProhibitedQuery(query) : handleQuery(query);
    }

    protected Predicate handleProhibitedQuery(org.apache.lucene.search.Query query) {
        Predicate result = null;
        if (query instanceof TermQuery) {
            result = handleProhibitedTermQuery((TermQuery) query);
        } else if (query instanceof WildcardQuery) {
            result = handleProhibitedWildcardQuery((WildcardQuery) query);
        } else if (query instanceof BooleanQuery) {
            result = handleBooleanQuery((BooleanQuery) query, true);
        } else if (query instanceof PrefixQuery) {
            result = handleProhibitedPrefixQuery((PrefixQuery) query);
        } else {
            throw new RuntimeException("Unsupported query: " + query);
        }
        return result;
    }

    protected Predicate handleProhibitedWildcardQuery(WildcardQuery wildcardQuery) {
        return not(handleWildcardQuery(wildcardQuery));
    }

    private Predicate handleProhibitedTermQuery(TermQuery query) {
        return not(handleTermQuery(query));
    }

    protected Predicate handleQuery(org.apache.lucene.search.Query query) {
        Predicate result = null;
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

    protected Predicate handlePhraseQuery(PhraseQuery phraseQuery) {
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
        List<Predicate> results = new ArrayList<>();
        for (String field : fields) {
            String value = parse(field, phrase) + "";
            Pattern pattern = Pattern.compile(Matcher.quoteReplacement(value), Pattern.CASE_INSENSITIVE);
            results.add(Functions.matches(field, pattern));
        }
        return any(results);
    }

    protected Predicate handleWildcardQuery(WildcardQuery wildcardQuery) {
        Term term = wildcardQuery.getTerm();
        List<Predicate> results = new ArrayList<>();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            String value = term.text().replace("?", "_QUESTION_MARK_").replace("*", "_STAR_");
            value = Matcher.quoteReplacement(value);
            value = value.replace("_QUESTION_MARK_", ".?").replace("_STAR_", ".*");
            Pattern pattern = Pattern.compile("^" + Matcher.quoteReplacement(value) + "$", Pattern.CASE_INSENSITIVE);
            results.add(Functions.matches(field, pattern));
        }
        return any(results);
    }

    protected Predicate handleProhibitedPrefixQuery(PrefixQuery prefixQuery) {
        return not(handlePrefixQuery(prefixQuery));
    }

    protected Predicate handlePrefixQuery(PrefixQuery prefixQuery) {
        List<Predicate> results = new ArrayList<>();
        Term term = prefixQuery.getPrefix();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            Object value = parse(field, term.text());
            Pattern pattern = Pattern.compile("^" + Matcher.quoteReplacement(value.toString()) + ".*", Pattern.CASE_INSENSITIVE);
            results.add(Functions.matches(field, pattern));
        }
        return any(results);
    }

    protected Predicate handleRangeQuery(TermRangeQuery rangeQuery) {
        List<Predicate> results = new ArrayList<>();
        List<String> fields = getField(rangeQuery.getField());
        for (String field : fields) {
            Object lower = parse(field, rangeQuery.getLowerTerm());
            Object upper = parse(field, rangeQuery.getUpperTerm());
            List<Predicate> expression = new ArrayList<>();
            if (upper != null) {
                expression.add(rangeQuery.includesUpper() ? lte(field, upper) : lt(field, upper));
            }
            if (lower != null) {
                expression.add(rangeQuery.includesLower() ? gte(field, lower) : gt(field, lower));
            }
            results.add(all(expression));
        }
        return any(results);
    }

    protected Predicate handleBooleanQuery(BooleanQuery boolQuery, boolean prohibited) {
        // De Morgan's Law:
        // "not (A and B)" is the same as "(not A) or (not B)"
        // "not (A or B)" is the same as "(not A) and (not B)".
        //
        Predicate result = null;
        List<BooleanClause> clauses = boolQuery.clauses();
        int nClauses = clauses.size();
        List<Predicate> children = new ArrayList<>();

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
        boolean conjunction = (andCount > 0 || notCount > 0);
        // inverse is out boolean clause is prohibited as a whole
        if (prohibited) {
            conjunction = !conjunction;
        }
        result = conjunction ? all(children) : any(children);

        return result;
    }

    private Predicate handleTermQuery(TermQuery termQuery) {
        List<Predicate> results = new ArrayList<>();
        Term term = termQuery.getTerm();
        List<String> fields = getField(term.field());
        for (String field : fields) {
            Object value = parse(field, term.text());
            results.add(Functions.term(field, value));
        }
        return any(results);
    }

    protected Object parse(String field, Object value) {
        if ("#".equals(value)) {
            value = params.next();
        }
        return schemaMapper.mapValue(field, value);
    }

    protected List<String> getField(String field) {
        return schemaMapper.mapField(field);
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
