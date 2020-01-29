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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.obiba.mica.spi.search.IndexFieldMapping;
import org.obiba.mica.spi.search.Indexable;
import org.obiba.mica.spi.search.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Persistable;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ESIndexer implements Indexer {

  private static final Logger log = LoggerFactory.getLogger(ESIndexer.class);

  private static final int MAX_SIZE = 10000;

  private final ESSearchEngineService esSearchService;

  public ESIndexer(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;
  }

  @Override
  public void index(String indexName, Persistable<String> persistable) {
    index(indexName, persistable, null);
  }

  @Override
  public void index(String indexName, Persistable<String> persistable, Persistable<String> parent) {
    log.debug("Indexing for indexName [{}] indexableObject [{}]", indexName, persistable);
    createIndexIfNeeded(indexName);
    getIndexRequestBuilder(indexName, persistable).setSource(toJson(persistable)).setParent(parent == null ? null : parent.getId()).execute().actionGet();
  }

  @Override
  public void index(String indexName, Indexable indexable) {
    index(indexName, indexable, null);
  }

  @Override
  public void index(String indexName, Indexable indexable, Indexable parent) {
    log.debug("Indexing for indexName [{}] indexableObject [{}]", indexName, indexable);
    createIndexIfNeeded(indexName);
    getIndexRequestBuilder(indexName, indexable).setSource(toJson(indexable)).setParent(parent == null ? null : parent.getId()).execute()
        .actionGet();
  }

  @Override
  synchronized public void reIndexAllIndexables(String indexName, Iterable<? extends Indexable> persistables) {
    if (hasIndex(indexName)) dropIndex(indexName);
    indexAllIndexables(indexName, persistables, null);
  }

  @Override
  synchronized public void reindexAll(String indexName, Iterable<? extends Persistable<String>> persistables) {
    if (hasIndex(indexName)) dropIndex(indexName);
    indexAll(indexName, persistables, null);
  }

  @Override
  public void indexAll(String indexName, Iterable<? extends Persistable<String>> persistables) {
    indexAll(indexName, persistables, null);
  }

  @Override
  public void indexAll(String indexName, Iterable<? extends Persistable<String>> persistables, Persistable<String> parent) {

    log.debug("Indexing all for indexName [{}] persistableObjectNumber [{}]", indexName, Iterables.size(persistables));

    createIndexIfNeeded(indexName);
    BulkRequestBuilder bulkRequest = getClient().prepareBulk();
    persistables.forEach(persistable -> bulkRequest
        .add(getIndexRequestBuilder(indexName, persistable).setSource(toJson(persistable)).setParent(parent == null ? null : parent.getId())));

    if (bulkRequest.numberOfActions() > 0) bulkRequest.execute().actionGet();
  }

  @Override
  public void indexAllIndexables(String indexName, Iterable<? extends Indexable> indexables) {
    indexAllIndexables(indexName, indexables, null);
  }

  @Override
  public void indexAllIndexables(String indexName, Iterable<? extends Indexable> indexables, @Nullable String parentId) {
    log.debug("Indexing all indexables for indexName [{}] persistableObjectNumber [{}]", indexName, Iterables.size(indexables));
    createIndexIfNeeded(indexName);
    BulkRequestBuilder bulkRequest = getClient().prepareBulk();
    indexables.forEach(indexable -> bulkRequest
        .add(getIndexRequestBuilder(indexName, indexable).setSource(toJson(indexable)).setParent(parentId)));

    if (bulkRequest.numberOfActions() > 0) bulkRequest.execute().actionGet();
  }

  @Override
  public void delete(String indexName, Persistable<String> persistable) {
    createIndexIfNeeded(indexName);
    getClient().prepareDelete(indexName, persistable.getClass().getSimpleName(), persistable.getId()).execute()
        .actionGet();
  }

  @Override
  public void delete(String indexName, Indexable indexable) {
    createIndexIfNeeded(indexName);
    getClient().prepareDelete(indexName, getType(indexable), indexable.getId()).execute().actionGet();
  }

  @Override
  public void delete(String indexName, String[] types, Map.Entry<String, String> termQuery) {
    QueryBuilder query = QueryBuilders.termQuery(termQuery.getKey(), termQuery.getValue());
    if (types != null) {
      createIndexIfNeeded(indexName);

      BulkRequestBuilder bulkRequest = getClient().prepareBulk();

      SearchRequestBuilder search = getClient().prepareSearch() //
          .setIndices(indexName) //
          .setTypes(types) //
          .setQuery(query) //
          .setSize(MAX_SIZE) //
          .setNoFields();

      SearchResponse response = search.execute().actionGet();

      for (SearchHit hit : response.getHits()) {
        for (String type : types) {
          DeleteRequestBuilder request = getClient().prepareDelete(indexName, type, hit.getId());
          if (hit.getFields() != null && hit.getFields().containsKey("_parent")) {
            String parent = hit.field("_parent").value();
            request.setParent(parent);
          }

          bulkRequest.add(request);
        }
      }

      try {
        bulkRequest.execute().get();
      } catch (InterruptedException | ExecutionException e) {
        //
      }
    }
  }

  @Override
  public void delete(String indexName, String type, Map.Entry<String, String> termQuery) {
    delete(indexName, type != null ? new String[]{type} : null, termQuery);
  }

  @Override
  public boolean hasIndex(String indexName) {
    return getClient().admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
  }

  @Override
  public void dropIndex(String indexName) {
    getClient().admin().indices().prepareDelete(indexName).execute().actionGet();
  }

  @Override
  public IndexFieldMapping getIndexfieldMapping(String indexName, String type) {
    return new IndexFieldMappingImpl(hasIndex(indexName) ? getContext(indexName, type) : null);
  }

  //
  // Private methods
  //

  private ReadContext getContext(String indexName, String indexType) {
    GetMappingsResponse result = getClient().admin().indices().prepareGetMappings(indexName).execute().actionGet();
    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = result.getMappings();
    MappingMetaData metaData = mappings.get(indexName).get(indexType);
    Object jsonContent = Configuration.defaultConfiguration().jsonProvider().parse(metaData.source().toString());
    return JsonPath.using(Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST)).parse(jsonContent);
  }

  private Client getClient() {
    return esSearchService.getClient();
  }

  private String toJson(Object obj) {
    try {
      return esSearchService.getObjectMapper().writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot serialize " + obj + " to ElasticSearch", e);
    }
  }

  private IndexRequestBuilder getIndexRequestBuilder(String indexName, Persistable<String> persistable) {
    return getClient().prepareIndex(indexName, persistable.getClass().getSimpleName(), persistable.getId());
  }

  private IndexRequestBuilder getIndexRequestBuilder(String indexName, Indexable indexable) {
    return getClient().prepareIndex(indexName, getType(indexable), indexable.getId());
  }

  private String getType(Indexable indexable) {
    return indexable.getMappingName() == null ? indexable.getClassName() : indexable.getMappingName();
  }

  private synchronized void createIndexIfNeeded(String indexName) {
    log.trace("Ensuring index existence for index {}", indexName);
    IndicesAdminClient indicesAdmin = getClient().admin().indices();
    if (!hasIndex(indexName)) {
      log.info("Creating index {}", indexName);

      Settings settings = Settings.builder() //
          .put(esSearchService.getIndexSettings())
          .put("number_of_shards", esSearchService.getNbShards()) //
          .put("number_of_replicas", esSearchService.getNbReplicas()).build();

      indicesAdmin.prepareCreate(indexName).setSettings(settings).execute().actionGet();
      esSearchService.getIndexConfigurationListeners().forEach(listener -> listener.onIndexCreated(esSearchService, indexName));
    }
  }

  private static class IndexFieldMappingImpl implements IndexFieldMapping {

    private final ReadContext context;

    IndexFieldMappingImpl(ReadContext ctx) {
      this.context = ctx;
    }

    @Override
    public boolean isAnalyzed(String fieldName) {
      boolean analyzed = false;
      if (context != null) {
        List<Object> result = context.read(String.format("$..%s..analyzed", fieldName.replaceAll("\\.", "..")));
        analyzed = result.size() > 0;
      }

      return analyzed;
    }

  }

}
