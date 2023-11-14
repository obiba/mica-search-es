/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.mapping;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;

import java.io.IOException;
import java.util.stream.Stream;

public class VariableIndexConfiguration extends AbstractIndexConfiguration {

  public VariableIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.PUBLISHED_VARIABLE_INDEX.equals(indexName)) {
      setMappingProperties(getClient(searchEngineService), indexName);
    }

    if (Indexer.PUBLISHED_HVARIABLE_INDEX.equals(indexName)) {
      setMappingProperties(getClient(searchEngineService), indexName);
    }
  }

  private void setMappingProperties(Client client, String indexName) {
    String variableType = Indexer.PUBLISHED_HVARIABLE_INDEX.equals(indexName) ? Indexer.HARMONIZED_VARIABLE_TYPE : Indexer.VARIABLE_TYPE;

    try {
      client.admin().indices().preparePutMapping(indexName).setType(
          variableType)
          .setSource(createMappingProperties(variableType)).execute().actionGet();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private XContentBuilder createMappingProperties(String type) throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(type);
    mapping.startArray("dynamic_templates").startObject().startObject("und").field("match", "und")
        .field("match_mapping_type", "string").startObject("mapping").field("type", "string")
        .field("index", "not_analyzed").endObject().endObject().endObject().endArray();

    // properties
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "containerId");
    createMappingWithoutAnalyzer(mapping, "studyId");
    createMappingWithoutAnalyzer(mapping, "populationId");
    createMappingWithoutAnalyzer(mapping, "dceId");
    createMappingWithoutAnalyzer(mapping, "datasetId");
    if (Indexer.HARMONIZED_VARIABLE_TYPE.equals(type)) {
      createMappingWithoutAnalyzer(mapping, "opalTableType");
      createMappingWithoutAnalyzer(mapping, "source");
    }
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    createMappingWithoutAnalyzer(mapping, "entityType");
    createMappingWithoutAnalyzer(mapping, "variableType");
    createMappingWithoutAnalyzer(mapping, "valueType");
    createMappingWithoutAnalyzer(mapping, "nature");
    createMappingWithoutAnalyzer(mapping, "sets");
    createMappingWithoutAnalyzer(mapping, "tableUid");

    // attributes from taxonomies
    try {
      mapping.startObject("attributes");
      mapping.startObject("properties");
      Stream.of(Indexer.VARIABLE_LOCALIZED_ANALYZED_FIELDS)
          .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
      mapping.endObject(); // properties
      mapping.endObject(); // attributes
    } catch (Exception e) {
      // ignore
    }

    mapping.endObject(); // properties

    mapping.endObject().endObject();
    return mapping;
  }

}
