/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.obiba.es.mica.query.AndQuery;
import org.obiba.es.mica.query.RQLJoinQuery;
import org.obiba.es.mica.query.RQLQuery;
import org.obiba.es.mica.results.ESResponseDocumentResults;
import org.obiba.es.mica.support.AggregationParser;
import org.obiba.es.mica.support.ESHitSourceMapHelper;
import org.obiba.mica.spi.search.QueryScope;
import org.obiba.mica.spi.search.Searcher;
import org.obiba.mica.spi.search.support.EmptyQuery;
import org.obiba.mica.spi.search.support.JoinQuery;
import org.obiba.mica.spi.search.support.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.obiba.mica.spi.search.QueryScope.AGGREGATION;
import static org.obiba.mica.spi.search.QueryScope.DETAIL;

public class ESSearcher implements Searcher {

  private static final Logger log = LoggerFactory.getLogger(Searcher.class);

  private final ESSearchEngineService esSearchService;

  private final AggregationParser aggregationParser = new AggregationParser();

  ESSearcher(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;
  }
  
  @Override
  public JoinQuery makeJoinQuery(String rql) {
    log.debug("makeJoinQuery: {}", rql);
    RQLJoinQuery joinQuery = new RQLJoinQuery(esSearchService.getConfigurationProvider(), esSearchService.getIndexer());
    joinQuery.initialize(rql);
    return joinQuery;
  }

  @Override
  public Query makeQuery(String rql) {
    log.debug("makeQuery: {}", rql);
    if (Strings.isNullOrEmpty(rql)) return new EmptyQuery();
    return new RQLQuery(rql);
  }

  @Override
  public Query andQuery(Query... queries) {
    return new AndQuery(queries);
  }

  @Override
  public DocumentResults query(String indexName, String type, Query query, QueryScope scope, List<String> mandatorySourceFields, Properties aggregationProperties, @Nullable IdFilter idFilter) throws IOException {

    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchRequestBuilder request = getClient().prepareSearch(indexName)
        .setTypes(type)
        .setSearchType(SearchType.QUERY_THEN_FETCH) //
        .setQuery(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .setFrom(query.getFrom()) //
        .setSize(scope == DETAIL ? query.getSize() : 0) //
        .addAggregation(AggregationBuilders.global(AGG_TOTAL_COUNT));

    List<String> sourceFields = getSourceFields(query, mandatorySourceFields);

    if (AGGREGATION == scope) {
      request.setNoFields();
    } else if (sourceFields != null) {
      if (sourceFields.isEmpty()) request.setNoFields();
      else request.setFetchSource(sourceFields.toArray(new String[sourceFields.size()]), null);
    }

    if (!query.isEmpty()) ((ESQuery) query).getSortBuilders().forEach(request::addSort);

    appendAggregations(request, query.getAggregationBuckets(), aggregationProperties);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().totalHits());

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults cover(String indexName, String type, Query query, Properties aggregationProperties, @Nullable IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchRequestBuilder request = getClient().prepareSearch(indexName)
        .setTypes(type)
        .setSearchType(SearchType.QUERY_THEN_FETCH) //
        .setQuery(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .setFrom(0) //
        .setSize(0) // no results needed for a coverage
        .setNoFields()
        .addAggregation(AggregationBuilders.global(Searcher.AGG_TOTAL_COUNT));

    appendAggregations(request, query.getAggregationBuckets(), aggregationProperties);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().totalHits());

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults cover(String indexName, String type, Query query, Properties aggregationProperties, Map<String, Properties> subAggregationProperties, @Nullable IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchRequestBuilder request = getClient().prepareSearch(indexName)
        .setTypes(type)
        .setSearchType(SearchType.QUERY_THEN_FETCH) //
        .setQuery(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .setFrom(0) //
        .setSize(0) // no results needed for a coverage
        .setNoFields()
        .addAggregation(AggregationBuilders.global(Searcher.AGG_TOTAL_COUNT));

    aggregationParser.getAggregations(aggregationProperties, subAggregationProperties).forEach(request::addAggregation);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().totalHits());

    return new ESResponseDocumentResults(response);
  }


  @Override
  public DocumentResults aggregate(String indexName, String type, Query query, Properties aggregationProperties, IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchRequestBuilder request = getClient().prepareSearch(indexName)
        .setTypes(type)
        .setSearchType(SearchType.QUERY_THEN_FETCH) //
        .setQuery(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .setFrom(0) //
        .setSize(0) // no results needed for a coverage
        .setNoFields()
        .addAggregation(AggregationBuilders.global(Searcher.AGG_TOTAL_COUNT));

    aggregationParser.getAggregations(aggregationProperties).forEach(request::addAggregation);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().totalHits());

    return new ESResponseDocumentResults(response);
  }

  private SearchRequestBuilder buildRequest(String indexName, String type, String rql, IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);

    RQLQuery query = new RQLQuery(rql);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : query.getQueryBuilder();

    SearchRequestBuilder request = getClient().prepareSearch()
      .setIndices(indexName)
      .setTypes(type)
      .setSearchType(DFS_QUERY_THEN_FETCH)
      .setQuery(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter))
      .setFrom(query.getFrom())
      .setSize(query.getSize());

    if (query.hasSortBuilders())
      query.getSortBuilders().forEach(request::addSort);
    else
      request.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));

    List<String> sourceFields = getSourceFields(query, new ArrayList<>());
    if (!sourceFields.isEmpty()) {
      request.setFetchSource(sourceFields.toArray(new String[sourceFields.size()]), null);
    }

    return request;
  }

  @Override
  public DocumentResults find(String indexName, String type, String rql, IdFilter idFilter) {
    SearchRequestBuilder request = buildRequest(indexName, type, rql, idFilter);
    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public List<DocumentResults> mfind(String indexName, String type, List<String> rqls, IdFilter idFilter) {
    MultiSearchRequestBuilder requests = getClient().prepareMultiSearch();
    rqls.forEach(rql -> requests.add(buildRequest(indexName, type, rql, idFilter)));

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, requests.toString());
    MultiSearchResponse responses = requests.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    List<DocumentResults> collect = Stream.of(responses.getResponses())
      .map(item -> new ESResponseDocumentResults(item.getResponse()))
      .collect(Collectors.toList());

    return collect;
  }

  @Override
  public DocumentResults count(String indexName, String type, String rql, IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    RQLQuery query = new RQLQuery(rql);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : query.getQueryBuilder();

    SearchRequestBuilder request = getClient().prepareSearch(indexName)
        .setTypes(type)
        .setQuery(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .setFrom(0)
        .setSize(0);

    query.getAggregations().forEach(field -> request.addAggregation(AggregationBuilders.terms(field).field(field).size(0)));

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public List<String> suggest(String indexName, String type, int limit, String locale, String queryString, String defaultFieldNamePattern) {
    String localizedFieldName = String.format(defaultFieldNamePattern, locale);
    String fieldName = localizedFieldName.replace(".analyzed", "");

    QueryBuilder queryExec = QueryBuilders.queryStringQuery(queryString)
        .defaultField(localizedFieldName)
        .defaultOperator(QueryStringQueryBuilder.Operator.OR);

    SearchRequestBuilder request = getClient().prepareSearch() //
        .setIndices(indexName) //
        .setTypes(type) //
        .setQuery(queryExec) //
        .setFrom(0) //
        .setSize(limit)
        .addSort(SortBuilders.scoreSort().order(SortOrder.DESC))
        .setFetchSource(new String[]{fieldName}, null);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    List<String> names = Lists.newArrayList();
    response.getHits().forEach(hit -> {
        String value = ESHitSourceMapHelper.flattenMap(hit).get(fieldName).toLowerCase();
        names.add(Joiner.on(" ").join(Splitter.on(" ").trimResults().splitToList(value).stream()
          .filter(str -> !str.contains("[") && !str.contains("(") && !str.contains("{") && !str.contains("]") && !str.contains(")") && !str.contains("}"))
          .map(str -> str.replace(":", "").replace(",", ""))
          .filter(str -> !str.isEmpty()).collect(Collectors.toList())));
      }
    );
    return names;
  }

  @Override
  public InputStream getDocumentById(String indexName, String type, String id) {
    QueryBuilder query = QueryBuilders.idsQuery(type).addIds(id);

    SearchRequestBuilder request = getClient().prepareSearch() //
        .setIndices(indexName) //
        .setTypes(type) //
        .setQuery(query);

    log.debug("Request: /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    if (response.getHits().totalHits() == 0) return null;
    return new ByteArrayInputStream(response.getHits().hits()[0].getSourceAsString().getBytes());
  }

  @Override
  public InputStream getDocumentByClassName(String indexName, String type, Class clazz, String id) {
    QueryBuilder query = QueryBuilders.queryStringQuery(clazz.getSimpleName()).field("className");
    query = QueryBuilders.boolQuery().must(query)
        .must(QueryBuilders.idsQuery(type).addIds(id));

    SearchRequestBuilder request = getClient().prepareSearch() //
        .setIndices(indexName) //
        .setTypes(type) //
        .setQuery(query);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    if (response.getHits().totalHits() == 0) return null;
    return new ByteArrayInputStream(response.getHits().hits()[0].getSourceAsString().getBytes());
  }

  @Override
  public DocumentResults getDocumentsByClassName(String indexName, String type, Class clazz, int from, int limit,
                                                 String sort, String order, String queryString,
                                                 TermFilter termFilter, IdFilter idFilter) {

    QueryBuilder query = QueryBuilders.queryStringQuery(clazz.getSimpleName()).field("className");
    if (queryString != null) {
      query = QueryBuilders.boolQuery().must(query).must(QueryBuilders.queryStringQuery(queryString));
    }

    QueryBuilder postFilter = getPostFilter(termFilter, idFilter);

    SearchRequestBuilder request = getClient().prepareSearch() //
        .setIndices(indexName) //
        .setTypes(type) //
        .setQuery(postFilter == null ? query : QueryBuilders.boolQuery().must(query).must(postFilter)) //
        .setFrom(from) //
        .setSize(limit);

    if (sort != null) {
      request.addSort(
          SortBuilders.fieldSort(sort).order(order == null ? SortOrder.ASC : SortOrder.valueOf(order.toUpperCase())));
    }

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults getDocuments(String indexName, String type, int from, int limit, @Nullable String sort, @Nullable String order, @Nullable String queryString, @Nullable TermFilter termFilter, @Nullable IdFilter idFilter, @Nullable List<String> fields, @Nullable List<String> excludedFields) {
    QueryStringQueryBuilder query = queryString != null ? QueryBuilders.queryStringQuery(queryString) : null;

    if (query != null && fields != null) fields.forEach(query::field);

    QueryBuilder postFilter = getPostFilter(termFilter, idFilter);

    QueryBuilder execQuery = postFilter == null ? query : query == null ? postFilter : QueryBuilders.boolQuery().must(query).filter(postFilter);

    if (excludedFields != null) {
      BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
      excludedFields.forEach(f -> boolQueryBuilder.mustNot(
          QueryBuilders.boolQuery().must(QueryBuilders.termQuery(f, "true")).must(QueryBuilders.existsQuery(f))));
      execQuery = boolQueryBuilder.must(execQuery);
    }

    SearchRequestBuilder request = getClient().prepareSearch() //
        .setIndices(indexName) //
        .setTypes(type) //
        .setQuery(execQuery) //
        .setFrom(from) //
        .setSize(limit);

    if (sort != null) {
      request.addSort(
          SortBuilders.fieldSort(sort).order(order == null ? SortOrder.ASC : SortOrder.valueOf(order.toUpperCase())));
    }

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
    SearchResponse response = request.execute().actionGet();
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public long countDocumentsWithField(String indexName, String type, String field) {
    BoolQueryBuilder builder = QueryBuilders.boolQuery()
        .should(QueryBuilders.existsQuery(field));

    SearchRequestBuilder request = getClient().prepareSearch(indexName) //
        .setTypes(type) //
        .setSearchType(DFS_QUERY_THEN_FETCH) //
        .setQuery(builder)
        .setFrom(0) //
        .setSize(0)
        .addAggregation(AggregationBuilders.terms(field.replaceAll("\\.", "-")).field(field).size(0));

    try {
      log.debug("Request /{}/{}: {}", indexName, type, request);
      if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, request.toString());
      SearchResponse response = request.execute().actionGet();
      log.debug("Response /{}/{}: {}", indexName, type, response);

      return response.getAggregations().asList().stream().flatMap(a -> ((Terms) a).getBuckets().stream())
          .map(a -> a.getKey().toString()).distinct().collect(Collectors.toList()).size();
    } catch (IndexNotFoundException e) {
      log.warn("Count of Studies With Variables failed", e);
      return 0;
    }
  }

  //
  // Private methods
  //

  private QueryBuilder getPostFilter(TermFilter termFilter, IdFilter idFilter) {
    QueryBuilder filter = null;

    if (idFilter != null)
      filter = getIdQueryBuilder(idFilter);

    if (termFilter != null && termFilter.getValue() != null) {
      QueryBuilder filterBy = QueryBuilders.termQuery(termFilter.getField(), termFilter.getValue());
      filter = filter == null ? filterBy : QueryBuilders.boolQuery().must(filter).must(filterBy);
    }

    return filter;
  }

  private QueryBuilder getIdQueryBuilder(IdFilter idFilter) {
    if (idFilter instanceof PathFilter) return getPathQueryBuilder((PathFilter) idFilter);
    QueryBuilder filter;
    Collection<String> ids = idFilter.getValues();
    if (ids.isEmpty())
      filter = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("id"));
    else if ("id".equals(idFilter.getField()))
      filter = QueryBuilders.idsQuery().ids(ids);
    else {
      BoolQueryBuilder orFilter = QueryBuilders.boolQuery();
      ids.forEach(id -> orFilter.should(QueryBuilders.termQuery(idFilter.getField(), id)));
      filter = orFilter;
      // FIXME filter = QueryBuilders.termsQuery(idFilter.getField(), ids);
    }
    return filter;
  }

  private QueryBuilder getPathQueryBuilder(PathFilter pathFilter) {
    List<QueryBuilder> includes = pathFilter.getValues().stream()
        .map(path -> path.endsWith("/") ? QueryBuilders.prefixQuery(pathFilter.getField(), path)
            : QueryBuilders.termQuery(pathFilter.getField(), path))
        .collect(Collectors.toList());
    List<QueryBuilder> excludes = pathFilter.getExcludedValues().stream()
        .map(path -> QueryBuilders.prefixQuery(pathFilter.getField(), path))
        .collect(Collectors.toList());

    BoolQueryBuilder includedFilter = QueryBuilders.boolQuery();
    includes.forEach(includedFilter::should);
    if (excludes.isEmpty()) return includedFilter;

    BoolQueryBuilder excludedFilter = QueryBuilders.boolQuery();
    excludes.forEach(excludedFilter::should);

    return QueryBuilders.boolQuery().must(includedFilter).mustNot(excludedFilter);
  }

  private void appendAggregations(SearchRequestBuilder requestBuilder, List<String> aggregationBuckets, Properties aggregationProperties) {
    Map<String, Properties> subAggregations = Maps.newHashMap();
    if (aggregationBuckets != null)
      aggregationBuckets.forEach(field -> subAggregations.put(field, aggregationProperties));
    aggregationParser.setLocales(esSearchService.getConfigurationProvider().getLocales());
    aggregationParser.getAggregations(aggregationProperties, subAggregations).forEach(requestBuilder::addAggregation);
  }

  /**
   * Returns the default source filtering fields. A NULL signifies the whole source to be included
   */
  private List<String> getSourceFields(Query query, List<String> mandatorySourceFields) {
    List<String> sourceFields = query.getSourceFields();

    if (sourceFields != null && !sourceFields.isEmpty()) {
      sourceFields.addAll(mandatorySourceFields);
    }

    return sourceFields;
  }

  private Client getClient() {
    return esSearchService.getClient();
  }

}
