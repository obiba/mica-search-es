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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Stream;

public class TaxonomyIndexConfiguration extends AbstractIndexConfiguration {

  private static final Logger log = LoggerFactory.getLogger(TaxonomyIndexConfiguration.class);

  public TaxonomyIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    XContentBuilder mapping = null;
    String type = null;
    
    try {

      switch (indexName) {
        case Indexer.TAXONOMY_INDEX:
          mapping = createTaxonomyMappingProperties();
          type = Indexer.TAXONOMY_TYPE;
          break;
        case Indexer.VOCABULARY_INDEX:
          mapping = createVocabularyMappingProperties();
          type = Indexer.TAXONOMY_VOCABULARY_TYPE;
          break;
        case Indexer.TERM_INDEX:
          mapping = createTermMappingProperties();
          type = Indexer.TAXONOMY_TERM_TYPE;
          break;
      }

      if (mapping != null) {
        getClient(searchEngineService).admin().indices().preparePutMapping(indexName) //
          .setType(type).setSource(mapping) //
          .execute().actionGet();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private XContentBuilder createTaxonomyMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(Indexer.TAXONOMY_TYPE);
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "target");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    Stream.of(Indexer.TAXONOMY_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject(); // properties
    mapping.endObject().endObject();
    return mapping;
  }

  private XContentBuilder createVocabularyMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
        .startObject(Indexer.TAXONOMY_VOCABULARY_TYPE);
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "target");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    createMappingWithAndWithoutAnalyzer(mapping, "taxonomyName");
    Stream.of(Indexer.TAXONOMY_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject(); // properties
    mapping.endObject().endObject();
    return mapping;
  }

  private XContentBuilder createTermMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
        .startObject(Indexer.TAXONOMY_TERM_TYPE);
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "target");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    createMappingWithAndWithoutAnalyzer(mapping, "taxonomyName");
    createMappingWithAndWithoutAnalyzer(mapping, "vocabularyName");
    Stream.of(Indexer.TAXONOMY_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject(); // properties
    mapping.endObject().endObject();
    return mapping;
  }

}
