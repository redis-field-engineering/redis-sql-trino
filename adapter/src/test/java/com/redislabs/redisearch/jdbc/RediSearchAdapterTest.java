/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redislabs.redisearch.jdbc;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.TestUtil;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

class RediSearchAdapterTest extends AbstractBaseTest {

  /**
   * Tests using a Calcite view.
   */
  @Test
  void view() {
    calciteAssert().query("select * from beers where name LIKE 'Sinister'").returns("name=Sinister " + "Minister Black IPA; abv=0.077; ounces=16.0; ibu=65.0; style=American IPA; " + "brewery_id=133; id=2346\nname=Sinister; abv=0.09; ounces=12.0; ibu=; style=American " + "Double / Imperial IPA; brewery_id=177; id=2263\nname=Sunset Amber; abv=0" + ".054000000000000006; ounces=12.0; ibu=; style=American Pale Ale (APA); brewery_id=204; " + "id=2182\nname=Grand Canyon Sunset Amber Ale; abv=0.054000000000000006; ounces=12.0; " + "ibu=; style=American Amber / Red Ale; brewery_id=536; id=143\n").returnsCount(4);
  }

  @Test
  void emptyResult() {
    CalciteAssert.that().with(newConnectionFactory()).query("select * from beers limit 0").returnsCount(0);

    CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers " +
        "where" + " " + "_MAP['Foo'] = '_MISSING_'").returnsCount(0);
  }

  @Test
  void basic() {
    CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers").runs();

    CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers " +
        "where" + " _MAP" + "['name'] LIKE 'Sinister'").returnsCount(4);

    CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers " +
        "where" + " _MAP['name'] LIKE 'SINISTER' OR _MAP['name'] LIKE 'FOREMAN'").returnsCount(DEFAULT_LIMIT_NUM);

    // limit 0
    CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers " +
        "limit" + " 0").returnsCount(0);
  }

  @Test
  void testSort() {
    final String explain =
        "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$1], " + "dir0=[ASC])\n   "
            + " RediSearchProject(name=[CAST(ITEM($0, 'name')):VARCHAR(200)], abv=[CAST" + "(ITEM"
            + "($0, 'abv')):FLOAT], ounces=[CAST(ITEM($0, 'ounces')):FLOAT], ibu=[CAST(ITEM($0, " + "'ibu')):INTEGER], style=[CAST(ITEM($0, 'style')):VARCHAR(200)], brewery_id=[CAST(ITEM" + "($0, 'brewery_id')):INTEGER], id=[CAST(ITEM($0, 'id')):VARCHAR(5)])\n      " + "RediSearchTableScan(table=[[redisearch, beers]])\n\n";

    calciteAssert().query("select * from beers order by abv").returnsCount(DEFAULT_LIMIT_NUM).returns(sortedResultSetChecker("abv", RelFieldCollation.Direction.ASCENDING)).explainContains(explain);
  }

  @Test
  void testSortLimit() {
    final String sql =
        "select name, abv from beers\n" + "order by abv offset 2 rows " + "fetch" + " next 3 " +
            "rows" + " only";
    calciteAssert().query(sql).returnsUnordered("name=Rocket Girl; abv=0.032\nname=Summer Brew; " + "abv=0.027999999999999997\nname=Totally Radler; abv=0.027000000000000003").queryContains(RediSearchChecker.rediSearchChecker("*"));
  }

  /**
   * Throws {@code AssertionError} if result set is not sorted by {@code column}.
   * {@code null}s are ignored.
   *
   * @param column    column to be extracted (as comparable object).
   * @param direction ascending / descending
   * @return consumer which throws exception
   */
  private static Consumer<ResultSet> sortedResultSetChecker(String column,
      RelFieldCollation.Direction direction) {
    Objects.requireNonNull(column, "column");
    return rset -> {
      try {
        final List<Comparable<?>> result = new ArrayList<>();
        while (rset.next()) {
          Object object = rset.getObject(column);
          if (object != null && !(object instanceof Comparable)) {
            final String message = String.format(Locale.ROOT, "%s is not comparable", object);
            throw new IllegalStateException(message);
          }
          if (object != null) {
            //noinspection rawtypes
            result.add((Comparable) object);
          }
        }
        for (int i = 0; i < result.size() - 1; i++) {
          //noinspection rawtypes
          final Comparable current = result.get(i);
          //noinspection rawtypes
          final Comparable next = result.get(i + 1);
          //noinspection unchecked
          final int cmp = current.compareTo(next);
          if (direction == RelFieldCollation.Direction.ASCENDING ? cmp > 0 : cmp < 0) {
            final String message = String.format(Locale.ROOT, "Column %s NOT sorted (%s): %s " +
                "(index:%d) > %s (index:%d) count: %d", column, direction, current, i, next,
                i + 1, result.size());
            throw new AssertionError(message);
          }
        }
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  /**
   * Sorting (and aggregating) directly on items without a view.
   * <p>Queries of type:
   * {@code select _MAP['a'] from elastic order by _MAP['b']}
   */
  @Test
  void testSortNoSchema() {
    CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers " +
        "order" + " by _MAP['name']").returnsCount(DEFAULT_LIMIT_NUM);

    CalciteAssert.that().with(newConnectionFactory()).query("select _MAP['style'] from " +
        "redisearch" + ".beers order by _MAP['name']").returnsCount(DEFAULT_LIMIT_NUM);

    CalciteAssert.that().with(newConnectionFactory()).query("select _MAP['name'] from redisearch" + ".beers where _MAP['style'] = 'American IPA' " + "order by _MAP['name']").returnsOrdered("EXPR$0=#002 American I.P.A.\nEXPR$0=#004 Session I.P.A.\nEXPR$0=113 IPA\nEXPR$0=11th Hour IPA\nEXPR$0=1916 Shore Shiver\nEXPR$0=2014 IPA Cicada Series\nEXPR$0=2020 IPA\nEXPR$0=21st Amendment IPA (2006)\nEXPR$0=3-Way IPA (2013)\nEXPR$0=360° India Pale Ale");
  }

  @Test
  void testFilterSort() {
    final String sql = "select * from beers where style = 'American IPA' and abv >= .05 order by " +
        "abv";
    final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$1], " +
        "dir0=[ASC])\n    RediSearchProject(name=[CAST(ITEM($0, 'name')):VARCHAR(200)], abv=[CAST" +
        "(ITEM($0, 'abv')):FLOAT], ounces=[CAST(ITEM($0, 'ounces')):FLOAT], ibu=[CAST(ITEM($0, " +
        "'ibu')):INTEGER], style=[CAST(ITEM($0, 'style')):VARCHAR(200)], brewery_id=[CAST(ITEM" +
        "($0, 'brewery_id')):INTEGER], id=[CAST(ITEM($0, 'id')):VARCHAR(5)])\n      " +
        "RediSearchFilter(condition=[AND(=(CAST(ITEM($0, 'style')):VARCHAR(200), 'American IPA')," +
        " >=(CAST(ITEM($0, 'abv')):FLOAT, 0.05:DECIMAL(2, 2)))])\n        RediSearchTableScan" +
        "(table=[[redisearch, beers]])\n\n";
    final String returns = "name=Citrafest; abv=0.05; ounces=16.0; ibu=45.0; style=American IPA; " +
        "brewery_id=27; id=2602\nname=Grand Circus IPA; abv=0.05; ounces=12.0; ibu=62.0; " +
        "style=American IPA; brewery_id=72; id=528\nname=Lasso; abv=0.05; ounces=12.0; ibu=; " +
        "style=American IPA; brewery_id=6; id=2645\nname=Grapefruit IPA; abv=0.05; ounces=12.0; " +
        "ibu=35.0; style=American IPA; brewery_id=13; id=2628\nname=Lil SIPA; abv=0.05; ounces=16" +
        ".0; ibu=55.0; style=American IPA; brewery_id=321; id=1771\nname=Jah Mon; abv=0.05; " +
        "ounces=12.0; ibu=100.0; style=American IPA; brewery_id=43; id=2579\nname=Dayman IPA; " +
        "abv=0.05; ounces=12.0; ibu=; style=American IPA; brewery_id=43; id=2048\nname=TailGate " +
        "IPA; abv=0.05; ounces=24.0; ibu=44.0; style=American IPA; brewery_id=449; " +
        "id=663\nname=TailGate IPA; abv=0.05; ounces=12.0; ibu=44.0; style=American IPA; " +
        "brewery_id=449; id=662\nname=South Bay Session IPA; abv=0.05; ounces=16.0; ibu=; " +
        "style=American IPA; brewery_id=33; id=2034";
    final String query = "(@style:{American IPA} @abv:[0.05 inf])";
    calciteAssert().query(sql).returnsOrdered(returns).queryContains(RediSearchChecker.rediSearchChecker(query)).explainContains(explain);
  }

  @Test
  void testBeers() {
    calciteAssert().query("select name, style from beers").returnsCount(DEFAULT_LIMIT_NUM);
  }

  @Test
  void testFilterReversed() {
    calciteAssert().query("select name, abv from beers where .1 < abv order by name").limit(2).returnsUnordered("name=4Beans; abv=0.1\nname=Csar; abv=0.12");
    calciteAssert().query("select name, abv from beers where abv > .1 order by name").limit(2).returnsUnordered("name=4Beans; abv=0.1\nname=Csar; abv=0.12");
  }


}
