/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.obiba.mica.spi.search.Searcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ESSearcher implements Searcher {

  private static final Logger log = LoggerFactory.getLogger(Searcher.class);

  private final ESSearchEngineService esSearchService;

  public ESSearcher(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;
  }

  @Override
  public SearchRequestBuilder prepareSearch(String... indices) {
    return esSearchService.getClient().prepareSearch(indices);
  }

  @Override
  public List<String> suggest(String indexName, String type, int limit, String locale, String queryString, String defaultFieldName) {
    String fieldName = defaultFieldName + "." + locale;

    QueryBuilder queryExec = QueryBuilders.queryStringQuery(queryString)
        .defaultField(fieldName + ".analyzed")
        .defaultOperator(QueryStringQueryBuilder.Operator.OR);

    SearchRequestBuilder search = esSearchService.getClient().prepareSearch() //
        .setIndices(indexName) //
        .setTypes(type) //
        .setQuery(queryExec) //
        .setFrom(0) //
        .setSize(limit)
        .addSort(SortBuilders.scoreSort().order(SortOrder.DESC))
        .setFetchSource(new String[]{fieldName}, null);

    log.debug(String.format("Request /%s/%s", indexName, type));
    if (log.isTraceEnabled()) log.trace(String.format("Request /%s/%s: %s", indexName, type, search.toString()));
    SearchResponse response = search.execute().actionGet();

    List<String> names = Lists.newArrayList();
    response.getHits().forEach(hit -> {
          String value = ((Map<String, Object>) hit.getSource().get(defaultFieldName)).get(locale).toString().toLowerCase();
          names.add(Joiner.on(" ").join(Splitter.on(" ").trimResults().splitToList(value).stream()
              .filter(str -> !str.contains("[") && !str.contains("(") && !str.contains("{") && !str.contains("]") && !str.contains(")") && !str.contains("}"))
              .map(str -> str.replace(":", "").replace(",", ""))
              .filter(str -> !str.isEmpty()).collect(Collectors.toList())));
        }
    );
    return names;
  }
}
