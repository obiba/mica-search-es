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

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.obiba.mica.spi.search.support.Query;

import java.util.List;

public interface ESQuery extends Query {

  boolean hasLimit();

  boolean hasQueryBuilder();

  QueryBuilder getQueryBuilder();

  boolean hasSortBuilders();

  List<SortBuilder> getSortBuilders();

}
