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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.obiba.mica.spi.search.Searcher;

public class ESSearcher implements Searcher{

  private final ESSearchEngineService esSearchService;

  public ESSearcher(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;
  }

  @Override
  public SearchRequestBuilder prepareSearch(String... indices) {
    return esSearchService.getClient().prepareSearch(indices);
  }

  @Override
  public AdminClient admin() {
    return esSearchService.getClient().admin();
  }
}
