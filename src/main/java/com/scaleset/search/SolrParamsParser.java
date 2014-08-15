package com.scaleset.search;

import javax.servlet.http.HttpServletRequest;

import static com.scaleset.search.QueryBuilder.toInteger;

public class SolrParamsParser {

    public QueryBuilder parse(HttpServletRequest request) {
        QueryBuilder qb = new QueryBuilder();
        qb.q(request.getParameter("q"));
        parsePaging(request, qb);
        parseSortParams(request, qb);
        parseFacets(request, qb);
        return qb;
    }

    void parseFacetMinCount(HttpServletRequest request, Aggregation fb) {
        int fallback = toInteger(request.getParameter("facet.mincount"), 0);
        String field = fb.getString("field");
        int minCount = toInteger(request.getParameter("f." + field + ".facet.mincount"), fallback);
        fb.property("min_doc_count", minCount);
    }

    void parseFacetOffset(HttpServletRequest request, Aggregation agg) {
    }

    void parseFacetSort(HttpServletRequest request, Aggregation agg) {
        agg.property("sort", Sort.Type.Lexical);
        String field = agg.getString("field");
        String facet_sort = request.getParameter("f." + field + ".facet.sort");
        if (facet_sort == null) {
            facet_sort = request.getParameter("facet.sort");
        }
        if ("count".equals(facet_sort)) {
            agg.property("sort", Sort.Type.Count);
        }
    }

    void parseFacets(HttpServletRequest request, QueryBuilder qb) {
        String[] fields = request.getParameterValues("facet.field");
        if (fields != null) {
            for (String field : fields) {
                Aggregation facet = parseFieldFacet(request, field);
                if (facet != null) {
                    qb.aggregation(facet);
                }
            }
        }
    }

    Aggregation parseFieldFacet(HttpServletRequest request, String facetField) {
        Aggregation agg = new Aggregation();
        agg.setType("field");
        parseFacetSort(request, agg);
        parseFacetMinCount(request, agg);
        parseLimit(request, agg);
        parseFacetOffset(request, agg);
        return agg;
    }

    void parsePaging(HttpServletRequest request, QueryBuilder qb) {
        int limit = toInteger(request.getParameter("rows"), 10);
        limit = toInteger(request.getParameter("limit"), limit);
        int offset = toInteger(request.getParameter("start"), 0);
        offset = toInteger(request.getParameter("offset"), offset);
        qb.offset(offset);
        qb.limit(limit);
    }

    void parseLimit(HttpServletRequest request, Aggregation agg) {
    }

    protected void parseSortParams(HttpServletRequest request, QueryBuilder qb) {
        String query_sort = request.getParameter("sort");
        if (query_sort != null) {
            for (String sort : query_sort.split(",")) {
                String[] sort_parts = sort.trim().split(" ");
                if (sort_parts.length == 1) {
                    String field = sort_parts[0].trim();
                    if (!field.isEmpty()) {
                        qb.sort(sort_parts[0]);
                    }
                } else if (sort_parts.length == 2) {
                    String field = sort_parts[0].trim();
                    if (!field.isEmpty()) {
                        qb.sort(sort_parts[0]);
                    }
                    Sort.Direction direction = "asc".equals(sort_parts[1]) ? Sort.Direction.Ascending : Sort.Direction.Descending;
                    qb.sort(new Sort(field, direction));
                }
            }
        }
    }

}
