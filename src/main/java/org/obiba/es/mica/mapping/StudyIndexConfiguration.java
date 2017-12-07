/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.mapping;

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.obiba.mica.spi.search.TaxonomyTarget;
import org.obiba.opal.core.domain.taxonomy.Taxonomy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class StudyIndexConfiguration extends AbstractIndexConfiguration {
  private static final Logger log = LoggerFactory.getLogger(StudyIndexConfiguration.class);

  public StudyIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.DRAFT_STUDY_INDEX.equals(indexName) || Indexer.PUBLISHED_STUDY_INDEX.equals(indexName)) {

      try {
        getClient(searchEngineService).admin().indices().preparePutMapping(indexName).setType(Indexer.STUDY_TYPE)
            .setSource(createMappingProperties()).execute().actionGet();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private XContentBuilder createMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(Indexer.STUDY_TYPE);
    startDynamicTemplate(mapping);
    dynamicTemplateExcludeFieldFromSearch(mapping, "parent_id", "*Memberships.parentId");
    endDynamicTemplate(mapping);

    mapping.startObject("properties");
    appendMembershipProperties(mapping);
    Taxonomy taxonomy = getTaxonomy();
    addLocalizedVocabularies(taxonomy, "name", "acronym");
    addStaticVocabularies(taxonomy, //
        "populations.dataCollectionEvents.start.yearMonth", //
        "populations.dataCollectionEvents.end.yearMonth");
    List<String> ignore = Lists.newArrayList(
        "memberships.investigator.person.fullName",
        "memberships.investigator.person.institution.name.und",
        "memberships.contact.person.fullName",
        "memberships.contact.person.institution.name.und"
    );
    addTaxonomyFields(mapping, taxonomy, ignore);
    mapping.endObject();

    return mapping;
  }

  @Override
  protected TaxonomyTarget getTarget() {
    return TaxonomyTarget.STUDY;
  }
}
