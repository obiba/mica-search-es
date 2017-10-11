/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.results;

import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link Terms.Bucket} wrapper.
 */
public class ESDocumentTermsBucket implements Searcher.DocumentTermsBucket {
  private final Terms.Bucket bucket;

  public ESDocumentTermsBucket(Terms.Bucket bucket) {
    this.bucket = bucket;
  }

  @Override
  public long getDocCount() {
    return bucket.getDocCount();
  }

  @Override
  public String getKeyAsString() {
    return bucket.getKeyAsString();
  }

  @Override
  public List<Searcher.DocumentAggregation> getAggregations() {
    return bucket.getAggregations().asList().stream().map(ESDocumentAggregation::new).collect(Collectors.toList());
  }
}
